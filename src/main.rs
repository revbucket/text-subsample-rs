
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use rand::Rng;
use dashmap::{DashMap, DashSet};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::BufRead;

use std::collections::HashMap;
use clap::{Parser, Subcommand};
use anyhow::{Result, Error};
use std::time::Instant;
use serde_json::Value;
use std::path::{PathBuf};
use serde::{Deserialize, Serialize};
use rayon::prelude::*;
use crate::io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, get_output_filename, has_json_extension};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;



pub mod s3;
pub mod io;

const SIG_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/*=================================================================
=                                  ARGS                           =
=================================================================*/


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[clap(subcommand)]
    command: Commands,

    #[arg(long, default_value_t=0)]
    threads: usize,
}


#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]
    BuildConfig {
        // Builds a config mapping document path -> usize id
        #[arg(required=true, long, num_args=1..)]
        input: Vec<PathBuf>,

        #[arg(required=true, long)]
        output: PathBuf
    },

    BuildExact { 
        // Creates a hash signature for each doc pointed to in the config
        #[arg(required=true, long)]
        config: PathBuf,

        #[arg(required=true, long)]
        output: PathBuf,
    },

    Subsample {
        #[arg(required=true, long)]
        config: PathBuf, 

        #[arg(required=true, long)]
        sig_loc: PathBuf,

        #[arg(required=true, long)]
        output: PathBuf,

        #[arg(required=true, long)]
        ratio: f64,

        #[arg(required=true, long, default_value_t=usize::MAX)]
        max_size: usize,

        #[arg(long, default_value_t=false)]
        nodup: bool


    }
}



/*=================================================================
=                           UTILS.                                =
=================================================================*/

fn build_pbar(num_items: usize, units: &str) -> ProgressBar {
    let mut template = String::from(units);
    template.push_str(" {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]");
    let pbar = ProgressBar::new(num_items as u64)
        .with_style(
            ProgressStyle::with_template(&template).unwrap()
        );
    pbar.inc(0);
    pbar
}



/*=================================================================
=                         CONFIG HELPERS                          =
=================================================================*/
#[derive(Serialize, Deserialize)]
pub struct Config {
    pub input: Vec<PathBuf>,
    pub indices: HashMap<PathBuf, usize>,
    // Need to have these attributes for compatibility with minhash-rs
    pub sig_size: usize,
    pub path_size: usize,
    pub line_size: usize
}

impl Config {
    pub fn new(input: &Vec<PathBuf>, num_docs: usize, max_lines_per_path: usize) -> Result<Self, Error> {
        // First gather all files 
        let paths = expand_dirs(input.clone(), None).unwrap();
        let num_paths = paths.len();
        let path_size = 0;
        let line_size = 0;
        let sig_size = 0;
        let indices : HashMap<PathBuf, usize> = paths.iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), i))
            .collect();
        Ok(Config {input: input.clone(),
                          indices, 
                          sig_size,
                          path_size,
                          line_size})
    }

    pub fn save(&self, save_loc: &PathBuf) -> Result<(), Error> {
        let json_bytes = serde_json::to_vec(self).unwrap();
        write_mem_to_pathbuf(&json_bytes, &save_loc)
    }

    pub fn load(load_loc: &PathBuf) -> Result<Self, Error> {
        let json_bytes = read_pathbuf_to_mem(&load_loc).unwrap();
        let cursor = json_bytes.into_inner();
        let binding = cursor.into_inner();
        let contents = binding.as_slice();
        let config: Config = serde_json::from_slice(&contents).unwrap();     
        Ok(config)
    }

    pub fn get_chunk(&self, chunk_id: usize, num_chunks: usize) -> Vec<(PathBuf, usize)> {
        let chunk : Vec<(PathBuf, usize)> = self.indices.iter()
             .filter(|(_k, v)| *v % num_chunks == chunk_id)
             .map(|(k, v)| (k.clone(), *v))
             .collect();
        chunk       
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }
}





/*=================================================================
=                            EXACT HELPERS                        =
=================================================================*/

fn exact_hash_path(path: &PathBuf, doc_idx: usize) -> Result<Vec<((usize, usize), usize)>, Error> {
    let mut trips: Vec<((usize, usize), usize)> = Vec::new();
    let data = read_pathbuf_to_mem(path).unwrap();
    for (line_num, line) in data.lines().enumerate() {
        let line = line.unwrap();
        let json: Value = serde_json::from_str(&line).unwrap();
        let text = json["text"].as_str().unwrap();
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let hash_val = hasher.finish() as usize;
        trips.push(((doc_idx, line_num), hash_val));
    }

    Ok(trips)
}

/*=================================================================
=                            SUBSAMPLE HELPERS                    =
=================================================================*/

fn load_triples(sig_loc: &PathBuf) -> Result<Vec<((usize, usize), usize)>, Error> {
    // First gather all chunks 
    let cc_ext = ".cc.bin";
    let mut cc_paths = expand_dirs(vec![sig_loc.clone()], Some(&[&cc_ext])).unwrap();
    cc_paths.sort();
    
    // Read the chunks into memory 
    let data_read_pbar = build_pbar(cc_paths.len(), "CC Chunks (loading)");
    let cc_chunks: Vec<Vec<u8>> = cc_paths.par_iter().map(|p| {
        let chunk_contents = read_pathbuf_to_mem(&p).unwrap().into_inner().into_inner();
        data_read_pbar.inc(1);
        chunk_contents
    }).collect();

    // Concatenate chunks
    let mut cc_bytes = Vec::new();
    let cat_pbar = build_pbar(cc_chunks.len(), "CC Chunks (concat2)");
    for chunk in cc_chunks {
        cc_bytes.extend(chunk);
        cat_pbar.inc(1);
    }

    // Deserialize
    let ccs = bincode::deserialize(&cc_bytes).unwrap();
    Ok(ccs)
}

fn trips_into_groups(triples: Vec<((usize, usize), usize)>) -> DashMap<usize, Vec<Vec<(usize, usize)>>> {
    // First gather into groups by signature 
    let sig_groups: DashMap<usize, Vec<(usize, usize)>> = DashMap::new();
    let sig_pbar = build_pbar(sig_groups.len(), "Signatures");
    triples.into_par_iter()
        .for_each(|((doc_id, line_num), sig)| {
            sig_groups.entry(sig).or_default().push((doc_id, line_num));
            sig_pbar.inc(1);
    });

    let groups : DashMap<usize, Vec<Vec<(usize, usize)>>> = DashMap::new();
    let group_pbar = build_pbar(sig_groups.len(), "Groups");
    sig_groups.into_par_iter()
        .for_each(|(_, v)| {
            groups.entry(v.len()).or_default().push(v);
            group_pbar.inc(1);
        });

    groups
}

fn build_profile(groups: &DashMap<usize, Vec<Vec<(usize, usize)>>>) -> HashMap<usize, f64> {
    // First get total number of docs
    let total_docs: f64 = groups.iter().map(|entry| entry.key() * entry.value().len()).sum::<usize>() as f64;

    let profile: HashMap<usize, f64> = groups
        .iter()
        .map(|entry| {
            (*entry.key(), 
             (entry.value().len() * entry.key()) as f64 / total_docs)
        })
        .collect();

    profile
}


fn subsample_groups(groups: DashMap<usize, Vec<Vec<(usize, usize)>>>, ratio: f64, max_size: usize, nodup: bool) 
-> DashMap<usize, Vec<Vec<(usize, usize)>>> {
    let flat_groups: Vec<Vec<(usize, usize)>> = groups
        .into_par_iter()
        .flat_map(|(_, v)| v)
        .collect();

    let subsampled_groups: DashMap<usize, Vec<Vec<(usize, usize)>>> = DashMap::new();
    let pbar = build_pbar(flat_groups.len(), "Groups (subsampling)");
    flat_groups.into_par_iter()
        .for_each(|g| {
            let mut rng = rand::thread_rng();
            if nodup {
                let new_group: Vec<(usize, usize)> = g.iter()
                    .filter(|_| rng.gen_bool(ratio))
                    .cloned()
                    .collect();
                subsampled_groups.entry(new_group.len()).or_default().push(new_group)
            } else if g.len() <= max_size && rng.gen_bool(ratio) {
                subsampled_groups.entry(g.len()).or_default().push(g)    
            }
            pbar.inc(1);
        });

    subsampled_groups

}

fn collect_survivors(groups: DashMap<usize, Vec<Vec<(usize, usize)>>>) -> DashMap<usize, DashSet<usize>> {
    // Maps surviving groups into a {doc_id -> set(line_num,...)}
    let survivors: DashMap<usize, DashSet<usize>> = DashMap::new();

    let flat_groups: Vec<Vec<(usize, usize)>> = groups
        .into_par_iter()
        .flat_map(|(_, v)| v)
        .collect();

    let pbar = build_pbar(flat_groups.len(), "Groups (survivors)");
    flat_groups.into_par_iter()
        .for_each(|g| {
            for (doc_id, line_num) in g {
                survivors.entry(doc_id).or_default().insert(line_num);
            }
        pbar.inc(1);
        });

    survivors
}


fn scrub_dataset(config: Config, lines_to_survive: DashMap<usize, DashSet<usize>>, output: &PathBuf) -> Result<(usize, usize), Error> {
    let documents_seen = AtomicUsize::new(0);
    let documents_removed = AtomicUsize::new(0);
    let pbar = build_pbar(lines_to_survive.len(), "Paths");
    config.indices.par_iter().for_each(|(path, idx)| {
        if lines_to_survive.contains_key(idx) {
            let output_filename = get_output_filename(&config.input, path, output)            ;
            let survivors = lines_to_survive.get(idx).map(|v| v.value().clone()).unwrap();
            let (path_seen, path_removed) = keep_survivors(path, &output_filename, survivors).unwrap();
            documents_seen.fetch_add(path_seen, Ordering::SeqCst);
            documents_removed.fetch_add(path_removed, Ordering::SeqCst);
        }
        pbar.inc(1);
    });
    Ok((documents_seen.into_inner(), documents_removed.into_inner()))
}

fn keep_survivors(input_filename: &PathBuf, output_filename: &PathBuf, survivors: DashSet<usize>) -> Result<(usize, usize), Error> {
    let data = read_pathbuf_to_mem(input_filename).unwrap();
    let mut lines_seen = 0;
    let mut lines_removed = 0;
    let mut output_bytes = Vec::new();
    let mut line_num = 0;
    for line in data.lines() {
        let line = line?;
        lines_seen += 1;
        if survivors.contains(&line_num) {
            output_bytes.extend(line.as_bytes());
            output_bytes.push(b'\n');            
        } else {
            lines_removed += 1;
        }
        line_num += 1;
    }
    if output_bytes.len() == 0 {
        return Ok((lines_seen, lines_removed))
    }
    write_mem_to_pathbuf(&output_bytes, output_filename).unwrap();
    Ok((lines_seen, lines_removed))
}



/*=================================================================
=                             COMMANDS                            =
=================================================================*/

fn build_config(input: &Vec<PathBuf>, output: &PathBuf) -> Result<(), Error> {
    // Build and save the path lookup
    println!("Building config...");
    let config = Config::new(input, 0, 0).unwrap();
    println!("Collected {:?} paths", config.len());
    let output = if has_json_extension(output) {
        output.clone() 
    } else {
        output.clone().join("config.json.gz")
    };
    config.save(&output)
}

fn build_exact(config: &PathBuf, output: &PathBuf) -> Result<(), Error> {
    // Computes exact hashes (as usize) of all docs and saves the triple of (doc_id, line_id, hash)
    println!("Starting exact hashing");
    let start_main = Instant::now();   
    let config = Config::load(config).unwrap();

    println!("Computing hashes for all docs...");
    let start_hash = Instant::now();
    let indices: Vec<(PathBuf, usize)> = config.indices.iter().map(|(k,v)| (k.clone(),*v)).collect();
    let pbar = build_pbar(indices.len(), "Paths");
    let signatures: Vec<((usize, usize), usize)> = indices
        .into_par_iter()
        .flat_map(|(path, doc_idx)| {            
            let trips = exact_hash_path(&path, doc_idx).unwrap();
            pbar.inc(1);
            trips
        })
        .collect();
    let total_docs_hashed = signatures.len();        
    println!("Hashed all docs in {:?} (s)", start_hash.elapsed().as_secs());

    println!("Saving hashes...");
    let start_save = Instant::now();
    let serialized_hashes = bincode::serialize(&signatures).unwrap();
    serialized_hashes.par_chunks(SIG_CHUNK_SIZE)
        .enumerate()
        .for_each(|(idx, chunk)| {
            let part_name = output.clone().join(format!("sig_part{:08}.cc.bin", idx));
            write_mem_to_pathbuf(chunk, &part_name).unwrap();
    });
    println!("Saved hashes in {:?} (s)", start_save.elapsed().as_secs());


    println!("-------------------------");
    println!("Completing exact hashing");
    println!("Computed hashes for {:?} docs", total_docs_hashed);
    println!("Total runtime: {:?} (s)", start_main.elapsed().as_secs());
    return Ok(());        
}


fn subsample(config: &PathBuf, sig_loc: &PathBuf, output: &PathBuf, ratio: f64, max_size: usize, nodup: bool) 
-> Result<(), Error> {
    println!("Starting subsample");
    let start_main = Instant::now();
    let config = Config::load(config).unwrap();

    // Step 1: load triples from sig_loc
    println!("Loading triples...");
    let start_tripload = Instant::now();
    let triples = load_triples(sig_loc).unwrap();
    println!("Loaded triples in {:?} (s)", start_tripload.elapsed().as_secs());

    // Step 2: group into {cc_size: Vec<Vec<(doc_id, line_num)>>}
    println!("Grouping triples...");
    let start_tripgroup = Instant::now();
    let groups = trips_into_groups(triples);
    let input_profile = build_profile(&groups);
    println!("Grouped triples in {:?} (s)", start_tripgroup.elapsed().as_secs());

    // Step 3: Subsample
    println!("Starting group subsample");
    let start_subsample = Instant::now();
    let subsampled_groups = subsample_groups(groups, ratio, max_size, nodup);
    let output_profile = build_profile(&subsampled_groups);
    println!("Subsampled groups in {:?} (s)", start_subsample.elapsed().as_secs());

    // Step 4: Collect survivor lines
    println!("Collecting survivors");
    let start_survivors = Instant::now();
    let survivors = collect_survivors(subsampled_groups);
    println!("Collected survivors rin {:?} (s)", start_survivors.elapsed().as_secs());

    // Step 5: Modify dataset 
    println!("Modifying dataset");
    let start_scrub = Instant::now();
    let (docs_seen, docs_removed) = scrub_dataset(config, survivors, output).unwrap();
    println!("Modified dataset in {:?} (s)", start_scrub.elapsed().as_secs());

    // Step 6: Save profiles 
    println!("Saving profiles");
    let start_profsave = Instant::now();
    let input_prof_json = serde_json::to_string(&input_profile).unwrap().into_bytes();
    let input_prof_filename = output.clone().join("input_profile.json");
    write_mem_to_pathbuf(&input_prof_json, &input_prof_filename).unwrap();
    let output_prof_json = serde_json::to_string(&output_profile).unwrap().into_bytes();
    let output_prof_filename = output.clone().join("output_profile.json");
    write_mem_to_pathbuf(&output_prof_json, &output_prof_filename).unwrap();
    println!("Saved profiles in {:?} (s)", start_profsave.elapsed().as_secs());


    println!("-------------------------");
    println!("Completing subsampling");
    println!("Output dataset is {:?} of input", (docs_seen - docs_removed) as f64 / docs_seen as f64);
    println!("Total runtime: {:?} (s)", start_main.elapsed().as_secs());
    return Ok(());        
}

/*=================================================================
=                                 MAIN                            =
=================================================================*/


fn main() {
    let args = ArgParser::parse();
    let threads = args.threads;
    if threads != 0 {
        std::env::set_var("RAYON_NUM_THREADS", threads.to_string());
    }

    let result = match &args.command {
        Commands::BuildConfig {input, output} => {
            build_config(input, output)
        },

        Commands::BuildExact {config, output} => {
            build_exact(config, output)
        },

        Commands::Subsample {config, sig_loc, output, ratio, max_size, nodup} => {
            subsample(config, sig_loc, output, *ratio, *max_size, *nodup)
        }
        _ => {Ok(())}
    };

    result.unwrap()
}