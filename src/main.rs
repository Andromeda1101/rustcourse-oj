use actix_web::HttpResponse;
use actix_web::{get, middleware::Logger, post, put, web, App, HttpServer, Responder};
use chrono::{DateTime, NaiveDateTime};
use chrono::Utc;
use env_logger;
use log;
use serde::{Deserialize, Serialize};
use clap::Parser;
use serde_json::to_string_pretty;
use wait_timeout::ChildExt;
use std::fs::read_to_string;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::mem::size_of_val;
use std::{clone, process};
use std::io;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};
use rusqlite::{Connection,params};

//this block defines global variables
lazy_static::lazy_static! {
    //save jobs
    static ref JOB_LIST: Arc<Mutex<Vec<Job>>> = Arc::new(Mutex::new(Vec::new()));
    //save users
    static ref USERS_LIST: Arc<Mutex<Vec<User>>> = Arc::new(Mutex::new(Vec::new()));
    //save contests
    static ref CONTESTS_LIST: Arc<Mutex<Vec<Contest>>> = Arc::new(Mutex::new(Vec::new()));
    //save submitting amount for each contest[index] by (user_id, submission_amount)
    static ref CONTESTS_SUB_LIMIT: Arc<Mutex<Vec<Vec<(usize, usize)>>>> = Arc::new(Mutex::new(Vec::new()));
    //save start argument to make it easy to approach
    static ref ARGL: Arc<Mutex<Argu>> = Arc::new(Mutex::new(Argu{config: None, flush_data: false}));
}

//define struct for input and output in request below
#[derive(Parser,Clone)]
#[command(version, author, about, long_about = None)]
struct Argu {
    #[arg(long, short = 'c', value_parser = clap::value_parser!(String))]
    config: Option<String>,
    #[arg(long, short = 'f')]
    flush_data: bool,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Config {
    server: Server,
    problems: Vec<Problem>,
    languages: Vec<Language>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Server {
    bind_address: Option<String>,
    bind_port: Option<u16>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Problem {
    id: usize,
    name: String,
    #[serde(rename = "type")]
    ty: String,
    misc: Option<Misc>,
    cases: Vec<Case>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Misc{
    special_judge: Option<Vec<String>>,
    dynamic_ranking_ratio: Option<f64>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct PostUser {
    id: Option<usize>,
    name: String,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct User {
    id: usize,
    name: String,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Case {
    score: f64,
    input_file: String,
    answer_file: String,
    time_limit: u64,
    memory_limit: u64,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Language {
    name: String,
    file_name: String,
    command: Vec<String>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct PostJob {
    source_code: String,
    language: String,
    user_id: usize,
    contest_id: usize,
    problem_id: usize,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Job {
    id: usize,
    created_time: String,
    updated_time: String,
    submission: PostJob,
    state: String,
    result: String,
    score: f64,
    cases: Vec<CaseResult>
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct CaseResult {
    id: usize,
    result: String,
    time: u128,
    memory: u64,
    info: String
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct MyError {
    code: i32,
    reason: String,
    message: String,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct GetJob {
    user_id: Option<usize>,
    user_name: Option<String>,
    contest_id: Option<usize>,
    problem_id: Option<usize>,
    language: Option<String>,
    from: Option<String>,
    to: Option<String>,
    state: Option<String>,
    result: Option<String>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct PostContest {
    id: Option<usize>,
    name: String,
    from: String,
    to: String,
    problem_ids: Vec<usize>,
    user_ids: Vec<usize>,
    submission_limit: usize,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Contest {
    id: usize,
    name: String,
    from: String,
    to: String,
    problem_ids: Vec<usize>,
    user_ids: Vec<usize>,
    submission_limit: usize,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct GetRankArg {
    scoring_rule: Option<String>,
    tie_breaker: Option<String>,
}

#[derive(Clone)]
#[derive(Serialize, Deserialize)]
struct Rank {
    user: User,
    rank: usize,
    scores: Vec<f64>,
}

//used for test to ensure the server works well
#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    log::info!(target: "greet_handler", "Greeting {}", name);
    format!("Hello {name}!")
}

//basic function part
//check if the post /jobs contents is legal
fn check_post_job(postjob: PostJob, config: web::Data<Config>)-> Result<(Language, Problem), MyError> {
    let mut cnt = 0; //counter
    //save for language infomation used
    let mut savelang = Language{ name: String::new(), file_name: String::new(), command: Vec::new()};
    //save for problem infomation used
    let mut savecase = Problem{cases: Vec::new(), id: 0, name: String::new(), ty: String::new(), misc: None };
    //check if language in config
    for i in config.languages.clone() {
        if postjob.language == i.name {
            cnt += 1;
            savelang = i.clone();
            break;
        }
    }
    if cnt == 0 { return Err(MyError{ reason: "ERR_NOT_FOUND".to_string(), code: 3, message: format!("Language {} Not Found",postjob.language).to_string(),}); }
    cnt = 0;
    //check if problem in config
    for i in config.problems.clone() {
        if postjob.problem_id == i.id {
            cnt += 1;
            savecase = i.clone();
            break;
        }
    }
    if cnt == 0 { return Err(MyError{ reason: "ERR_NOT_FOUND".to_string(), code: 3, message: format!("Problem {} Not Found",postjob.problem_id),}); }
    if postjob.user_id >= USERS_LIST.lock().unwrap().len() {
        return Err(MyError{ reason: "ERR_NOT_FOUND".to_string(), code: 3, message: format!("User {} Not Found",postjob.user_id),});
    }
    //check if contest is available
    if postjob.contest_id != 0 {
        
        if postjob.contest_id >= CONTESTS_LIST.lock().unwrap().len() {
            return Err(MyError{ reason: "ERR_INVALID_ARGUMENT".to_string(), code: 1, message: format!("HTTP 400 Bad Request"),});
        } else {
            let mut cnt = 0;
            let mut userplace: usize = 0;
            let this_cont = CONTESTS_LIST.lock().unwrap()[postjob.contest_id].clone();
            for i in 0..this_cont.user_ids.len() {
                if this_cont.user_ids[i] == postjob.user_id {
                    cnt = 1;
                    userplace = i;
                    break;
                }
            }
            if cnt == 0 {
                return Err(MyError{ reason: "ERR_INVALID_ARGUMENT".to_string(), code: 1, message: format!("HTTP 400 Bad Request"),});
            }
            cnt = 0;
            for i in CONTESTS_LIST.lock().unwrap()[postjob.contest_id].problem_ids.clone() {
                if i == postjob.problem_id {
                    cnt = 1;
                    break;
                }
            }
            if cnt == 0 {
                return Err(MyError{ reason: "ERR_INVALID_ARGUMENT".to_string(), code: 1, message: format!("HTTP 400 Bad Request"),});
            }
            if NaiveDateTime::parse_from_str(&CONTESTS_LIST.lock().unwrap()[postjob.contest_id].from.clone(),"%Y-%m-%dT%H:%M:%S%.3fZ").unwrap().and_utc() 
                    > Utc::now() {
                return Err(MyError{ reason: "ERR_INVALID_ARGUMENT".to_string(), code: 1, message: format!("HTTP 400 Bad Request"),});
            }
            if NaiveDateTime::parse_from_str(&CONTESTS_LIST.lock().unwrap()[postjob.contest_id].to.clone(),"%Y-%m-%dT%H:%M:%S%.3fZ").unwrap().and_utc() 
                    < Utc::now() {
                return Err(MyError{ reason: "ERR_INVALID_ARGUMENT".to_string(), code: 1, message: format!("HTTP 400 Bad Request"),});
            }
            if CONTESTS_SUB_LIMIT.lock().unwrap()[postjob.contest_id][userplace].1 >= this_cont.submission_limit {
                return Err(MyError{ reason: "ERR_RATE_LIMIT".to_string(), code: 4, message: format!("HTTP 400 Bad Request"),});
            } else {
                CONTESTS_SUB_LIMIT.lock().unwrap()[postjob.contest_id][userplace].1 += 1;
                if data_update_for_subn(postjob.contest_id, to_string_pretty(&CONTESTS_SUB_LIMIT.lock().unwrap()[postjob.contest_id].clone()).unwrap()).is_err() {
                    return Err(MyError{ reason: "ERR_EXTERNAL".to_string(), code: 5, message: format!("HTTP 400 Internal Server Error"),});
                };
            }
        }
    } else {
        CONTESTS_SUB_LIMIT.lock().unwrap()[postjob.contest_id][postjob.user_id].1 += 1;
        if data_update_for_subn(postjob.contest_id, to_string_pretty(&CONTESTS_SUB_LIMIT.lock().unwrap()[postjob.contest_id].clone()).unwrap()).is_err() {
            return Err(MyError{ reason: "ERR_EXTERNAL".to_string(), code: 5, message: format!("HTTP 400 Internal Server Error"),});
        };
    }
    Ok((savelang, savecase))
}

//build program for oj judge
fn build_test(postjob: PostJob, langu: Language)-> Result<ExitStatus, MyError> {
    let _ = std::fs::remove_dir_all("tmp");
    let _ = std::fs::create_dir("tmp");
    let mut _inputfile = std::fs::File::create("tmp/".to_owned()+&langu.file_name.clone());
    match _inputfile {
        Err(_e) => return Err(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() }),
        Ok(mut inputfile) => {
            let _ = inputfile.write(postjob.source_code.as_bytes());
        },
    }
    let mut comargs: Vec<String> = Vec::new();
    for i in langu.command.clone() {
        if i == "%INPUT%" {
            comargs.push("tmp/".to_owned()+&langu.file_name.clone());
        } else if i == "%OUTPUT%" {
            comargs.push("tmp/out_put_program".to_string());
        } else {
            comargs.push(i);
        }
    }
    comargs.remove(0);
    let _status = process::Command::new(langu.command[0].clone())
                    .args(comargs)
                    .status();
    match _status {
        Err(_e) => return Err(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() }),
        Ok(status) => return Ok(status),
    }
}

//save program result temporarily
struct Runstate {
    status: i32,
    runtime: u128,
    memory: u64,
}

//run program and return result
async fn run_test(caseinfo: Case, caseid: usize)-> Result<Runstate, io::Error> {
    let in_file = File::open(caseinfo.input_file)?;
    let out_file = File::create(format!("tmp/{}.out",caseid))?;
    let mut child = process::Command::new(format!("tmp/out_put_program"))
                     .stdin(process::Stdio::from(in_file))
                     .stdout(process::Stdio::from(out_file))
                     .stderr(process::Stdio::null())
                     .spawn()?;
    let tili = std::time::Duration::from_micros(caseinfo.time_limit);
    let runt_before = std::time::Instant::now();
    //check child if run time is out of limit
    match child.wait_timeout(tili).unwrap() {
        Some(status) => if status.success() {
            let runt = runt_before.elapsed();
            return Ok(Runstate{ status: 0, runtime: runt.as_micros(), memory: size_of_val(&child) as u64});
        } else {
            let runt = runt_before.elapsed();
            return Ok(Runstate{ status: 1, runtime: runt.as_micros(), memory: size_of_val(&child) as u64});
        }
        None => {
                // child hasn't exited yet
                child.kill().unwrap();
                child.wait().unwrap();
                return Ok(Runstate{ status: 2, runtime: caseinfo.time_limit as u128, memory: size_of_val(&child) as u64});
                }
        };
}

//the following 3 parts make compare between output and answer in std/strict/spj mode
fn cmp_output_std(caseinfo: Case, caseid: usize)-> Result<bool,io::Error> {
    let out_put_file = File::open(format!("tmp/{}.out",caseid))?;
    let ans_file = File::open(caseinfo.answer_file)?;
    let output_buffer_reader = BufReader::new(out_put_file);
    let ans_buffer_reader = BufReader::new(ans_file);
    let mut out_put: Vec<String> = Vec::new();
    let mut ans: Vec<String> = Vec::new();
    //analyse file contents by lines and delete the space code on the end
    for line in output_buffer_reader.lines() {
        let mut read_line = line?.clone();
            while let pop_i = read_line.pop() {
                if pop_i.is_none(){
                    break;
                } else if pop_i.unwrap() != ' ' {
                    read_line.push(pop_i.unwrap());
                    break;
                }
            }
        out_put.push(read_line);
    }
    for line in ans_buffer_reader.lines() {
        let mut read_line = line?.clone();
            while let pop_i = read_line.pop() {
                if pop_i.is_none(){
                    break;
                } else if pop_i.unwrap() != ' ' {
                    read_line.push(pop_i.unwrap());
                    break;
                }
        }
        ans.push(read_line);
    }
    println!("{:?}",out_put);
    println!("{:?}",ans);
    
        if ans.len() > out_put.len() {
            let m = ans.len() - out_put.len();
            for i in 0..m {
                out_put.push(String::from(""));
            }
        } else {
            let m = out_put.len() - ans.len();
            for i in 0..m {
                ans.push(String::from(""));
            }
        }
    let l = ans.len();
    for i in 0..l {
        if ans[i] != out_put[i] {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cmp_output_strict(caseinfo: Case, caseid: usize)-> Result<bool,io::Error> {
    let output_reader = read_to_string(format!("tmp/{}.out",caseid))?;
    let ans_reader = read_to_string(caseinfo.answer_file)?;
    if ans_reader == output_reader {
        return Ok(true);
    } else {return Ok(false);}
}

fn special_judge(caseinfo: Case, caseid: usize, spj: Vec<String>)-> Result<(String, String),io::Error> {
    let mut comargs: Vec<String> = Vec::new();
    for i in spj.clone() {
        if i == "%ANSWER%" {
            comargs.push(caseinfo.answer_file.clone());
        } else if i == "%OUTPUT%" {
            comargs.push(format!("tmp/{}.out",caseid));
        } else {
            comargs.push(i);
        }
    }
    comargs.remove(0);
    //run spj and get the result
    let spj_output_u8 = process::Command::new(spj[0].clone())
                            .args(comargs)
                            .output()?.stdout;
    let spj_output = String::from_utf8(spj_output_u8);
    if spj_output.as_ref().is_err() {
        return Err(io::Error::new(io::ErrorKind::Other, ""));
    } else {
        let _spj_out = spj_output.unwrap();
        let spj_out: Vec<&str> = _spj_out.split("\n").collect();
        let b = spj_out[0].to_string();
        if b != "Accepted" && b != "Wrong Answer" && b != "Time Limit Exceeded" && b != "Memory Limit Exceeded" && b != "Runtime Error" && b != "System Error" {
            return Err(io::Error::new(io::ErrorKind::Other, ""));
        } else { return Ok((b,spj_out[1].to_string())); }
    }   
}

//server for request post /jobs
#[post("/jobs")]
async fn post_jobs(postjob: web::Json<PostJob>, config: web::Data<Config>) -> impl Responder {
    //check request contents and get infomation
    let _saveinfo = check_post_job(postjob.clone(), config.clone());
    match _saveinfo {
        Err(e) => { 
            if e.code == 3 { return actix_web::HttpResponse::NotFound().json(e.clone()); }
            else if e.code == 5 || e.code == 6 { return actix_web::HttpResponse::InternalServerError().json(e.clone()); } 
            else { return actix_web::HttpResponse::BadRequest().json(e.clone()); }
        },
        Ok(saveinfo) => {
            //create a new job item
            let ins_time = Utc::now();
            let mut job_res = Job{ id:JOB_LIST.lock().unwrap().len(),
                created_time:ins_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                updated_time:ins_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                submission:postjob.clone(), 
                state: String::from("Running"), 
                result: String::from("Waiting"), 
                score: 0.0, 
                cases: Vec::new(), };
                for i in 0..=saveinfo.1.cases.len() {
                    job_res.cases.push( CaseResult{ id: i.clone(), result: "Waiting".to_string(), time: 0, memory: 0, info: "".to_string() } );
                }
            //build program
            match build_test(postjob.clone(), saveinfo.0.clone()) {
                Err(e) => {return actix_web::HttpResponse::BadRequest().json(e);},
                Ok(status) => if !status.success() {
                    job_res.state = String::from("Finished");
                    job_res.result = String::from("Compilation Error");
                    job_res.cases[0].result = "Compilation Error".to_string();
                    return actix_web::HttpResponse::Ok().json(job_res);
                }
            };
            job_res.cases[0].result = "Compilation Success".to_string();
            let mut dy_ratio: f64 = 0.0;
            if saveinfo.1.ty == "dynamic_ranking" {
                if saveinfo.1.misc.as_ref().is_none() {
                    actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });

                } else {
                    if saveinfo.1.misc.clone().unwrap().dynamic_ranking_ratio.is_none() {
                        actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });

                    } else {
                        dy_ratio = saveinfo.1.misc.clone().unwrap().dynamic_ranking_ratio.unwrap();
                    }
                }
            }
            for i in 0..saveinfo.1.cases.len() {
                //run program
                match run_test(saveinfo.1.cases[i].clone(), i).await {
                    Err(_e) => {
                        return actix_web::HttpResponse::InternalServerError().
                            json(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                    },
                    Ok(state) => {
                        // check the answer
                        match state.status {
                            0 => {
                                job_res.cases[i+1] = CaseResult{ id: i+1, result: "".to_string(), time: state.runtime, memory: state.memory, info: "".to_string() };
                                if state.memory > saveinfo.1.cases[i].memory_limit && saveinfo.1.cases[i].memory_limit > 0 {
                                    job_res.cases[i+1].result = "Memory Limit Exceeded".to_string();
                                } else {
                                    //run compare programs
                                    match &saveinfo.1.clone().ty as &str {
                                        "standard" | "dynamic_ranking" => match cmp_output_std(saveinfo.1.cases[i].clone(), i) {
                                            Ok(b) => {
                                                if b {
                                                    job_res.cases[i+1].result = "Accepted".to_string();
                                                    job_res.score += saveinfo.1.cases[i].score * (1.0-dy_ratio);
                                                } else {
                                                    job_res.cases[i+1].result = "Wrong Answer".to_string();
                                                }
                                            },
                                            Err(_e) => {
                                                return actix_web::HttpResponse::InternalServerError().
                                                    json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                                            },},
                                        "strict" => match cmp_output_strict(saveinfo.1.cases[i].clone(), i) {
                                            Ok(b) => {
                                                if b {
                                                    job_res.cases[i+1].result = "Accepted".to_string();
                                                    job_res.score += saveinfo.1.cases[i].score;
                                                } else {
                                                    job_res.cases[i+1].result = "Wrong Answer".to_string();
                                                }
                                            },
                                            Err(_e) => {
                                                return actix_web::HttpResponse::InternalServerError().
                                                    json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                                            },},
                                        "spj" => {
                                            if saveinfo.1.clone().misc.is_none() {
                                                return actix_web::HttpResponse::BadRequest()
                                                    .json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });
                                            } else {
                                                if saveinfo.1.clone().misc.unwrap().special_judge.is_none(){
                                                    return actix_web::HttpResponse::BadRequest()
                                                        .json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });
                                                } else {
                                                    match special_judge(saveinfo.1.cases[i].clone(), i, saveinfo.1.clone().misc.unwrap().special_judge.unwrap()) {
                                                        Err(_e) => {
                                                            job_res.cases[i+1].result = "SPJ Error".to_string();
                                                        },
                                                        Ok(b) => {
                                                            job_res.cases[i+1].result = b.0.clone();
                                                            job_res.cases[i+1].info = b.1.clone();
                                                            if b.0 == "Accepted" {
                                                                job_res.score += saveinfo.1.cases[i].score;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        &_ => return actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() }),
                                    
                                    };
                                    
                                }
                                
                            },
                            1 => {
                                job_res.cases[i+1].result = "Runtime Error".to_string();
                            },
                            2 => {
                                job_res.cases[i+1].result = "Time Limit Exceeded".to_string();
                            },
                            _ => return actix_web::HttpResponse::InternalServerError().
                            json(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() }),
                        }
                    }
                }
            }
            for i in 1..job_res.cases.len() {
                if job_res.cases[i].result != "Accepted" {
                    job_res.result = job_res.cases[i].result.clone();
                    break;
                }
            }
            if job_res.result == "Waiting" { job_res.result = "Accepted".to_string(); }
            job_res.state = "Finished".to_string();
            if data_insert("jobs".to_string(), to_string_pretty(&job_res).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            JOB_LIST.lock().unwrap().push(job_res.clone());
            return actix_web::HttpResponse::Ok().json(job_res);
        }
    }
    
}

//the following 2 are servers for request get /jobs
#[get("/jobs")]
async fn get_jobs(job_condi: web::Query<GetJob>)-> impl Responder {
    let mut find_res: Vec<Job>= Vec::new();
    if job_condi.from.is_some() {
        let jf = DateTime::parse_from_str(&job_condi.from.clone().unwrap(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ"));
        if jf.is_err() {
            return actix_web::HttpResponse::BadRequest().
                json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid argument from".to_string() });
        }
    }
    if job_condi.to.is_some() {
        let jt = DateTime::parse_from_str(&job_condi.to.clone().unwrap(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ"));
        if jt.is_err() {
            return actix_web::HttpResponse::BadRequest().
                json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid argument from".to_string() });
        }
    }
    for i in JOB_LIST.lock().unwrap().clone() {
        let mut cnt = 0;
        let mut is_no_argu = true;
        //check contents requirement
        if job_condi.user_id.is_some(){
            if i.submission.user_id == job_condi.user_id.unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.user_name.is_some() {
            is_no_argu = false;
            let mut tid: Option<usize> = None;
            for i in USERS_LIST.lock().unwrap().clone() {
                if i.name == job_condi.user_name.clone().unwrap() {
                    tid = Some(i.id);
                }
            }
            if tid.is_some() {
                if tid.unwrap() == i.submission.user_id {cnt += 1;}
            }
        }  else if job_condi.contest_id.is_some(){
            if i.submission.contest_id == job_condi.contest_id.unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.problem_id.is_some() {
            if i.submission.problem_id == job_condi.problem_id.unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.language.is_some() {
            if i.submission.language == job_condi.language.clone().unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.from.is_some() { 
            if DateTime::parse_from_str(&i.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap() 
                >= DateTime::parse_from_str(&job_condi.from.clone().unwrap(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.to.is_some() { 
            if DateTime::parse_from_str(&i.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap() 
                >= DateTime::parse_from_str(&job_condi.to.clone().unwrap(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.state.is_some() {
            if job_condi.state.as_ref().unwrap() != "Finished" {
                return actix_web::HttpResponse::BadRequest().
                json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid argument state".to_string() });
            }
            if i.state == job_condi.state.clone().unwrap() {cnt += 1;}
            is_no_argu = false;
        } else if job_condi.result.is_some() {
            if i.result == job_condi.result.clone().unwrap() {cnt += 1;}
            is_no_argu = false;
        }
        if cnt != 0 || is_no_argu { find_res.push(i.clone()); }
    }
    return actix_web::HttpResponse::Ok().json(find_res);
}

#[get("/jobs/{job_id}")]
async fn get_jobs_from_id(job_id: web::Path<usize>)-> impl Responder {
    if JOB_LIST.lock().unwrap().len() > *job_id {
        return actix_web::HttpResponse::Ok().json(JOB_LIST.lock().unwrap()[job_id.clone()].clone());
    } else {
        return actix_web::HttpResponse::NotFound().json(MyError{ code: 3, reason: "ERR_NOT_FOUND".to_string(), message: String::from(format!("Job {} not found.",job_id)) });
    }
    
}

//a reappearance for post /job
#[put("/jobs/{job_id}")]
async fn put_jobs(job_id: web::Path<usize>, config: web::Data<Config>)-> impl Responder {
    //get target jobs
    let mut _job_res: Option<Job> = None;
    for i in JOB_LIST.lock().unwrap().clone() {
        if i.id == *job_id {
            _job_res = Some(i.clone());
            break;
        }
    }
    if _job_res.is_none() {
        return actix_web::HttpResponse::NotFound()
            .json(MyError{ code: 3, reason: "ERR_NOT_FOUND".to_string(), message: String::from(format!("Job {} not found.",job_id)) });

    }
    //recheck
    let mut job_res = _job_res.unwrap();
    job_res.score = 0.0;
    job_res.state = "Running".to_string();
    job_res.result = "Waiting".to_string();
    job_res.cases.clear();
    let postjob = job_res.submission.clone();
    let _saveinfo = check_post_job(postjob.clone(), config.clone());
    match _saveinfo {
        Err(e) => { return actix_web::HttpResponse::NotFound().json(e); },
        Ok(saveinfo) => {
            for i in 0..=saveinfo.1.cases.len() {
                job_res.cases.push( CaseResult{ id: i.clone(), result: "Waiting".to_string(), time: 0, memory: 0, info: "".to_string() } );
            }
            match build_test(postjob.clone(), saveinfo.0.clone()) {
                Err(e) => {return actix_web::HttpResponse::BadRequest().json(e);},
                Ok(status) => if !status.success() {
                    job_res.state = String::from("Finished");
                    job_res.result = String::from("Compilation Error");
                    job_res.cases[0].result = "Compilation Error".to_string();
                    let ins_time = Utc::now();
                    job_res.updated_time = ins_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
                    JOB_LIST.lock().unwrap()[job_res.id] = job_res.clone();
                    return actix_web::HttpResponse::Ok().json(job_res);
                }
            };
            job_res.cases[0].result = "Compilation Success".to_string();
            let mut dy_ratio: f64 = 0.0;
            if saveinfo.1.ty == "dynamic_ranking" {
                if saveinfo.1.misc.as_ref().is_none() {
                    actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });

                } else {
                    if saveinfo.1.misc.clone().unwrap().dynamic_ranking_ratio.is_none() {
                        actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });

                    } else {
                        dy_ratio = saveinfo.1.misc.clone().unwrap().dynamic_ranking_ratio.unwrap();
                    }
                }
            }
            for i in 0..saveinfo.1.cases.len() {
                match run_test(saveinfo.1.cases[i].clone(), i).await {
                    Err(_e) => {
                        return actix_web::HttpResponse::InternalServerError().
                            json(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                    },
                    Ok(state) => {
                        match state.status {
                            0 => {
                                job_res.cases[i+1] = CaseResult{ id: i+1, result: "".to_string(), time: state.runtime, memory: state.memory, info: "".to_string() };
                                if state.memory > saveinfo.1.cases[i].memory_limit && saveinfo.1.cases[i].memory_limit > 0 {
                                    job_res.cases[i+1].result = "Memory Limit Exceeded".to_string();
                                } else {
                                    match &saveinfo.1.clone().ty as &str {
                                        "standard" | "dynamic_ranking" => match cmp_output_std(saveinfo.1.cases[i].clone(), i) {
                                            Ok(b) => {
                                                if b {
                                                    job_res.cases[i+1].result = "Accepted".to_string();
                                                    job_res.score += saveinfo.1.cases[i].score * (1.0-dy_ratio);
                                                } else {
                                                    job_res.cases[i+1].result = "Wrong Answer".to_string();
                                                }
                                            },
                                            Err(_e) => {
                                                return actix_web::HttpResponse::InternalServerError().
                                                    json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                                            },},
                                        "strict" => match cmp_output_strict(saveinfo.1.cases[i].clone(), i) {
                                            Ok(b) => {
                                                if b {
                                                    job_res.cases[i+1].result = "Accepted".to_string();
                                                    job_res.score += saveinfo.1.cases[i].score;
                                                } else {
                                                    job_res.cases[i+1].result = "Wrong Answer".to_string();
                                                }
                                            },
                                            Err(_e) => {
                                                return actix_web::HttpResponse::InternalServerError().
                                                    json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                                            },},
                                            "spj" => {
                                                if saveinfo.1.clone().misc.is_none() {
                                                    return actix_web::HttpResponse::BadRequest()
                                                        .json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });
                                                } else {
                                                    if saveinfo.1.clone().misc.unwrap().special_judge.is_none(){
                                                        return actix_web::HttpResponse::BadRequest()
                                                            .json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() });
                                                    } else {
                                                        match special_judge(saveinfo.1.cases[i].clone(), i, saveinfo.1.clone().misc.unwrap().special_judge.unwrap()) {
                                                            Err(_e) => {
                                                                job_res.cases[i+1].result = "SPJ Error".to_string();
                                                            },
                                                            Ok(b) => {
                                                                job_res.cases[i+1].result = b.0.clone();
                                                                job_res.cases[i+1].info = b.1.clone();
                                                                if b.0 == "Accepted" {
                                                                    job_res.score += saveinfo.1.cases[i].score;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                        &_ => return actix_web::HttpResponse::BadRequest().
                                            json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "HTTP 400 Bad Request".to_string() }),
                                    
                                    };
                                    
                                }
                                
                            },
                            1 => {
                                job_res.cases[i+1].result = "Runtime Error".to_string();
                            },
                            2 => {
                                job_res.cases[i+1].result = "Time Limit Exceeded".to_string();
                            },
                            _ => return actix_web::HttpResponse::InternalServerError().
                            json(MyError{ code: 6, reason: "ERR_INTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() }),
                        }
                    }
                }
            }
            for i in 1..job_res.cases.len() {
                if job_res.cases[i].result != "Accepted" {
                    job_res.result = job_res.cases[i].result.clone();
                    break;
                }
            }
            
            if job_res.result == "Waiting" { job_res.result = "Accepted".to_string(); }
            job_res.state = "Finished".to_string();
            let ins_time = Utc::now();
            job_res.updated_time = ins_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            if data_update("jobs".to_string(), job_id.clone(), to_string_pretty(&job_res).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            JOB_LIST.lock().unwrap()[job_res.id] = job_res.clone();
            return actix_web::HttpResponse::Ok().json(job_res);
        }
    }
}

//server for get /users
#[get("/users")]
async fn get_users()-> impl Responder {
    HttpResponse::Ok().json(USERS_LIST.lock().unwrap().clone())
}

//check if a name is already in use
fn find_user_name(name: String, id: Option<usize>)-> bool {
    for i in USERS_LIST.lock().unwrap().clone() {
        if id.is_some() && id.unwrap() == i.id { continue; }
        if i.name == name {
            return true;
        }
    }
    false
}

//server for post /users
#[post("/users")]
async fn post_users(postuser: web::Json<PostUser>)-> impl Responder {
    if postuser.id.as_ref().is_none() {
        //new a user
        if find_user_name(postuser.name.clone(), None) {
            return actix_web::HttpResponse::BadRequest().
                json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: format!("User name '{}' already exists.",postuser.name).to_string() });
        } else {
            let new_id = USERS_LIST.lock().unwrap().len();
            USERS_LIST.lock().unwrap().push(User { id: new_id, name: postuser.name.clone() });
            CONTESTS_SUB_LIMIT.lock().unwrap()[0].push( (new_id.clone(), 0) );
            
            if data_insert("users".to_string(), to_string_pretty(&User { id: new_id, name: postuser.name.clone() }).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            if data_update_for_subn(0, to_string_pretty(&CONTESTS_SUB_LIMIT.lock().unwrap()[0].clone()).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            return HttpResponse::Ok().json(User { id: new_id, name: postuser.name.clone() });
        }
    } else {
        //updated a user
        if find_user_name(postuser.name.clone(), postuser.id.clone()) {
            return actix_web::HttpResponse::BadRequest().
                    json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: format!("User name '{}' already exists.",postuser.name).to_string() });
        } else if postuser.id.unwrap() >= USERS_LIST.lock().unwrap().len() {
            return actix_web::HttpResponse::NotFound().
                    json(MyError{ code: 3, reason: "ERR_NOT_FOUND".to_string(), message: format!("User '{}' not find.",postuser.id.unwrap()).to_string() });
        } else {
            USERS_LIST.lock().unwrap()[postuser.id.unwrap()].name = postuser.name.clone();
            if data_update("users".to_string(), postuser.id.unwrap().clone(), to_string_pretty(&USERS_LIST.lock().unwrap()[postuser.id.unwrap()].clone()).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            return HttpResponse::Ok().json(User { id: postuser.id.unwrap(), name: postuser.name.clone() });
        }
    }
}

//check if post contents is legal
fn check_post_contests(postcon: PostContest, config: web::Data<Config>)-> Option<MyError> {
    //check users
    let mut save_us: Vec<u8> = Vec::new(); 
    save_us.resize(USERS_LIST.lock().unwrap().len(), 0);
    for i in postcon.user_ids {
        if save_us[i] != 0 {
            return Some( MyError{reason: "ERR_INVALID_ARGUMENT".to_string(), code:1, message: format!("Invalid argument user_ids")} );
        } else {
            save_us[i] = 1;
        }
        if i >= USERS_LIST.lock().unwrap().len() {
            return Some( MyError{reason: "ERR_NOT_FOUND".to_string(), code:3, message: format!("User {} not found.",i)} );
        }
    }
    //check problems
    let mut save_pro:Vec<usize> = Vec::new();
    for i in postcon.problem_ids {
        if save_pro.iter().position(|&x| x == i).is_some() {
            return Some( MyError{reason: "ERR_INVALID_ARGUMENT".to_string(), code:1, message: format!("Invalid argument problem_ids")} );
        } else { save_pro.push(i); }
        let mut m = false;
        for j in config.problems.clone() {
            if i == j.id { m = true; break;}
        }
        if !m {
            return Some( MyError{reason: "ERR_NOT_FOUND".to_string(), code:3, message: format!("Problem {} not found.",i)});
        }
    }
    None
}

//server for post /contents
#[post("/contests")]
async fn post_contests(postcon: web::Json<PostContest>, config: web::Data<Config>)-> impl Responder {
    if postcon.id.is_none() {
        let _che = check_post_contests(postcon.clone(), config.clone());
        if _che.is_some() {
            if _che.as_ref().unwrap().code == 3 {
                return actix_web::HttpResponse::NotFound().json(_che.unwrap().clone());
            } else if _che.as_ref().unwrap().code == 1 {
                return actix_web::HttpResponse::BadRequest().json(_che.unwrap().clone());
            } else {
                return actix_web::HttpResponse::InternalServerError().json(_che.unwrap().clone());
            }
        } else {
            //new a contest
            let l = CONTESTS_LIST.lock().unwrap().len();
            CONTESTS_LIST.lock().unwrap().push( Contest { id: l, name: postcon.name.clone(), from: postcon.from.clone(), to: postcon.to.clone(), problem_ids: postcon.problem_ids.clone(), user_ids: postcon.user_ids.clone(), submission_limit: postcon.submission_limit.clone() });
            CONTESTS_SUB_LIMIT.lock().unwrap().push(Vec::new());
            for i in postcon.user_ids.clone() {
                CONTESTS_SUB_LIMIT.lock().unwrap()[l].push((i, 0));
            }
            if data_insert("contests".to_string(), to_string_pretty(&CONTESTS_LIST.lock().unwrap()[l].clone()).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            if data_update_for_subn(l, to_string_pretty(&CONTESTS_SUB_LIMIT.lock().unwrap()[l].clone()).unwrap()).is_err() {
                return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
            }
            return actix_web::HttpResponse::Ok().json(CONTESTS_LIST.lock().unwrap()[l].clone());
        }
    } else {
        if postcon.id.unwrap() == 0 {
            return actix_web::HttpResponse::BadRequest().json(MyError{reason: "ERR_INVALID_ARGUMENT".to_string(), code:1, message: format!("Invalid contest id")});
        } else if postcon.id.unwrap() >= CONTESTS_LIST.lock().unwrap().len() {
            return actix_web::HttpResponse::NotFound().json(MyError{reason: "ERR_NOT_FOUND".to_string(), code:3, message: format!("Content {} not found.", postcon.id.unwrap())});

        } else {
            let _che = check_post_contests(postcon.clone(), config.clone());
            if _che.is_some() {
                if _che.as_ref().unwrap().code == 3 {
                    return actix_web::HttpResponse::NotFound().json(_che.unwrap().clone());
                } else if _che.as_ref().unwrap().code == 1 {
                    return actix_web::HttpResponse::BadRequest().json(_che.unwrap().clone());
                } else {
                    return actix_web::HttpResponse::InternalServerError().json(_che.unwrap().clone());
                }
            } else {
                //update a contest
                let l = postcon.id.unwrap().clone();
                CONTESTS_LIST.lock().unwrap()[l] = Contest { id: l.clone(), name: postcon.name.clone(), from: postcon.from.clone(), to: postcon.to.clone(), problem_ids: postcon.problem_ids.clone(), user_ids: postcon.user_ids.clone(), submission_limit: postcon.submission_limit.clone() };
                CONTESTS_SUB_LIMIT.lock().unwrap()[l] = Vec::new();
                for i in postcon.user_ids.clone() {
                    CONTESTS_SUB_LIMIT.lock().unwrap()[l].push((i, 0));
                }
                if data_update("contests".to_string(), l, to_string_pretty(&CONTESTS_LIST.lock().unwrap()[l].clone()).unwrap()).is_err() {
                    return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                }
                if data_update_for_subn(l, to_string_pretty(&CONTESTS_SUB_LIMIT.lock().unwrap()[l].clone()).unwrap()).is_err() {
                    return actix_web::HttpResponse::InternalServerError().json(MyError{ code: 5, reason: "ERR_EXTERNAL".to_string(), message: "HTTP 500 Internal Server Error".to_string() });
                }
                return actix_web::HttpResponse::Ok().json(CONTESTS_LIST.lock().unwrap()[l].clone());

            }
        }
    }
}

//the following 2 are servers for get /contests
#[get("/contests")]
async fn get_contests()-> impl Responder {
    let subvec = CONTESTS_LIST.lock().unwrap().clone()[1..].to_vec();
    HttpResponse::Ok().json(subvec)
}

#[get("/contests/{contestId}")]
async fn get_contests_from_id(contest_id: web::Path<usize>)-> impl Responder {
    if contest_id.clone() == 0 {
        return HttpResponse::BadRequest().json(MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid Contest id".to_string() });
    } else if contest_id.clone() >= CONTESTS_LIST.lock().unwrap().len() {
        return HttpResponse::NotFound().json(MyError{ code: 3, reason: "ERR_NOT_FOUND".to_string(), message: format!("Contest {} not found.", contest_id) });
    } else {
        return HttpResponse::Ok().json(CONTESTS_LIST.lock().unwrap()[contest_id.clone()].clone());
    }

}

//find jobs to use in rank in appointed rule 
fn find_job_for_rank(user_id: usize, problem_id: usize, contest_id: usize, rule: String)-> Option<Job> {
    let vl = JOB_LIST.lock().unwrap().clone();
    let mut tj: Option<Job> = None;
    for i in vl {
        if i.submission.user_id == user_id && i.submission.problem_id == problem_id
                && i.submission.contest_id == contest_id {
            if tj.as_ref().is_none() { 
                tj = Some(i.clone());
            }
            else {
                if rule == "latest" { tj = Some(i.clone()); }
                else {
                    if tj.clone().unwrap().score <= i.score {
                        tj = Some(i.clone());
                    } 
                }
            }
        } 
    }
    tj
}

//search for the shortest time to be used in calculating the dynamic score
//save all cases in one term
fn find_casetime_for_dy(problem_id: usize, contest: Contest, casesize: usize)-> Vec<u128> {
    let mut casetime: Vec<u128> = Vec::new();
    casetime.resize(casesize, 0 as u128);
    let vl = JOB_LIST.lock().unwrap().clone();
    for i in vl {
        if contest.id != 0 && contest.user_ids.clone().iter().find(|&&x| x == i.submission.user_id).is_none() { continue; }
        if i.submission.problem_id == problem_id && i.submission.contest_id == contest.id && i.result == "Accepted" {
            for j in 1..i.cases.len() {
                if casetime[j-1] == 0 {
                    casetime[j-1] = i.cases[j].time.clone();
                } else {
                    if casetime[j-1] > i.cases[j].time {
                        casetime[j-1] = i.cases[j].time.clone();
                    }
                }
            }
        }
    }
    casetime
}

//find jobs to use in rank by dynamic_ranking rule
fn find_job_for_dy(user_id: usize, problem: Problem, contest: Contest, rule: String)-> Option<Job> {
    let vl = JOB_LIST.lock().unwrap().clone();
    let mut tj: Option<Job> = None;
    for i in vl {
        if i.submission.user_id == user_id && i.submission.problem_id == problem.id && i.submission.contest_id == contest.id {
            if tj.as_ref().is_none() { 
                tj = Some(i.clone());
            }
            else {
                if tj.as_ref().unwrap().result == "Accepted" && i.result == "Accepted" {
                    tj = Some(i.clone());
                }
                else if rule == "latest" { tj = Some(i.clone()); }
                else {
                    if tj.clone().unwrap().score <= i.score {
                        tj = Some(i.clone());
                    } 
                }
            }
        } 
    }
    //use the job save above and add the dynamic score
    if tj.is_some() && tj.as_ref().unwrap().result == "Accepted" {
        let ratio = problem.misc.unwrap().dynamic_ranking_ratio.unwrap().clone();
        let casetime = find_casetime_for_dy(problem.id, contest, problem.cases.len());
        let mut tmp = tj.unwrap().clone();
        for i in 1..tmp.cases.len() {
            let sc: f64 = problem.cases[i-1].score * ratio * (casetime[i-1].clone() as f64) / (tmp.cases[i].time.clone() as f64);
            tmp.score += sc; 
        }
        tj = Some(tmp);
    }
    tj
}

fn this_problem(config: web::Data<Config>, id: usize)-> Option<Problem> {
    for i in config.problems.clone() {
       if i.id == id {
            return Some(i.clone());
       } 
    }
    None
}

//this is used to save infomation temporarily
#[derive(Clone)]
struct RankTmpSave {
    user: User,
    sub_time: Option<NaiveDateTime>,
    sub_count: usize,
    score: f64,
    jobs: Vec<Option<Job>>,
}

//server for get ///ranklist
#[get("/contests/{contestId}/ranklist")]
async fn get_rank(contest_id: web::Path<usize>, rank_arg: web::Query<GetRankArg>, config: web::Data<Config>)-> impl Responder {
    let mut rank_save: Vec<RankTmpSave> = Vec::new();
    let mut rule = String::from("latest");
    if rank_arg.scoring_rule.as_ref().is_some() { 
        rule = rank_arg.scoring_rule.clone().unwrap(); 
        if rule != "latest" && rule != "highest" {
            return HttpResponse::BadRequest().json( MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid argument scoring_rule".to_string() } );
        }
    }
    //save infomation used in ranking and search for submision count and submission time
    if contest_id.clone() != 0 {
        let this_contest = CONTESTS_LIST.lock().unwrap()[contest_id.clone()].clone();
        for i in 0..this_contest.user_ids.len() {
            let mut tmp_rank = RankTmpSave{ user: USERS_LIST.lock().unwrap()[this_contest.user_ids[i]].clone(), 
                                                         sub_time: None, 
                                                         sub_count: CONTESTS_SUB_LIMIT.lock().unwrap()[contest_id.clone()][i].1, 
                                                         score: 0.0, jobs: Vec::new() };
            for j in this_contest.problem_ids.clone() {
                let mut _fj = None;
                let this_problem = this_problem(config.clone(), j).unwrap();
                if this_problem.ty == "dynamic_ranking" {
                    _fj = find_job_for_dy(this_contest.user_ids[i].clone(), this_problem.clone(), this_contest.clone(), rule.clone());
                } else {
                    _fj = find_job_for_rank(this_contest.user_ids[i].clone(), j.clone(), contest_id.clone(), rule.clone());
                }
                if _fj.is_some() {
                    let fj = _fj.clone().unwrap();
                    if tmp_rank.sub_time.is_none() {
                        tmp_rank.sub_time = Some(NaiveDateTime::parse_from_str(&fj.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap());
                    } else if NaiveDateTime::parse_from_str(&fj.created_time.clone(),"%Y-%m-%dT%H:%M:%S%.3fZ").unwrap() >= tmp_rank.sub_time.unwrap() {
                        tmp_rank.sub_time = Some(NaiveDateTime::parse_from_str(&fj.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap());
                    }
                    tmp_rank.score += fj.score;
                }
                tmp_rank.jobs.push(_fj);
            }
            rank_save.push(tmp_rank);
            //rank according to score
            let mut l = rank_save.len() - 1;
            while l > 0 {
                if rank_save[l].score > rank_save[l-1].score {
                    let r1 = rank_save[l].clone();
                    let r2 = rank_save[l-1].clone();
                    rank_save[l] = r2;
                    rank_save[l-1] = r1;
                    l -= 1;
                } else {break;}
            }
        }
        
    } else {
        let mut pros = config.problems.clone();
        for i in 0..pros.len() {
            for j in 0..(pros.len()-i-1) {
                if pros[j+1].id < pros[j].id {
                    let p1 = pros[j].clone();
                    let p2 = pros[j+1].clone();
                    pros[j] = p2;
                    pros[j+1] = p1;
                }
            }
        }
        for i in USERS_LIST.lock().unwrap().clone() {
            let mut tmp_rank = RankTmpSave{ user: i.clone(), 
                                                         sub_time: None, 
                                                         sub_count: CONTESTS_SUB_LIMIT.lock().unwrap()[0][i.id].1, 
                                                         score: 0.0, jobs: Vec::new() };
            for j in pros.clone() {
                let mut _fj = None;
                if j.ty == "dynamic_ranking" {
                    _fj = find_job_for_dy(i.id.clone(), j.clone(), CONTESTS_LIST.lock().unwrap()[contest_id.clone()].clone(), rule.clone());
                } else {
                    _fj = find_job_for_rank(i.id.clone(), j.id.clone(), contest_id.clone(), rule.clone());
                }
                if _fj.is_some() {
                    let fj = _fj.clone().unwrap();
                    if tmp_rank.sub_time.is_none() {
                        tmp_rank.sub_time = Some(NaiveDateTime::parse_from_str(&fj.created_time.clone(),"%Y-%m-%dT%H:%M:%S%.3fZ").expect("not"));
                    } else if NaiveDateTime::parse_from_str(&fj.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap() >= tmp_rank.sub_time.unwrap() {
                        tmp_rank.sub_time = Some(NaiveDateTime::parse_from_str(&fj.created_time.clone(),&format!("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap());
                    }
                    tmp_rank.score += fj.score;
                }
                tmp_rank.jobs.push(_fj);
            }
            rank_save.push(tmp_rank);
            //rank according to score
            let mut l = rank_save.len() - 1;
            while l > 0 {
                if rank_save[l].score > rank_save[l-1].score {
                    let r1 = rank_save[l].clone();
                    let r2 = rank_save[l-1].clone();
                    rank_save[l] = r2;
                    rank_save[l-1] = r1;
                    l -= 1;
                } else {break;}
            }
        }
    }
    //slide the tie score part in rank
    let mut tie_i: usize = 0;
    while tie_i < rank_save.len() {
        let tie_sc = rank_save[tie_i].score.clone();
        let mut tie_end = tie_i;
        for j in (tie_i+1)..rank_save.len() {
            if rank_save[j].score != tie_sc {
                break;
            } else {
                tie_end = j;
            }
        }
        //rank in slide to break the tie
        while tie_end != tie_i {
            for j in 0..(tie_end-tie_i) {
                if rank_arg.tie_breaker.as_ref().is_none() {
                    if rank_save[tie_i+j+1].user.id < rank_save[tie_i+j].user.id {
                        let r1 = rank_save[tie_i+j].clone();
                        let r2 = rank_save[tie_i+j+1].clone();
                        rank_save[tie_i+j] = r2;
                        rank_save[tie_i+j+1] = r1;
                    }
                } else {
                    if rank_arg.tie_breaker.as_ref().unwrap() == "user_id" {
                        if rank_save[tie_i+j+1].user.id < rank_save[tie_i+j].user.id {
                            let r1 = rank_save[tie_i+j].clone();
                            let r2 = rank_save[tie_i+j+1].clone();
                            rank_save[tie_i+j] = r2;
                            rank_save[tie_i+j+1] = r1;
                        }
                    } else if rank_arg.tie_breaker.as_ref().unwrap() == "submission_time" {
                        if rank_save[tie_i+j+1].sub_time.is_some() {
                            if rank_save[tie_i+j].sub_time.is_none() {
                                let r1 = rank_save[tie_i+j].clone();
                                let r2 = rank_save[tie_i+j+1].clone();
                                rank_save[tie_i+j] = r2;
                                rank_save[tie_i+j+1] = r1;
                            } else if rank_save[tie_i+j+1].sub_time.unwrap() < rank_save[tie_i+j].sub_time.unwrap() {
                                let r1 = rank_save[tie_i+j].clone();
                                let r2 = rank_save[tie_i+j+1].clone();
                                rank_save[tie_i+j] = r2;
                                rank_save[tie_i+j+1] = r1;
                            } else if rank_save[tie_i+j+1].sub_time.unwrap() == rank_save[tie_i+j].sub_time.unwrap() {
                                if rank_save[tie_i+j+1].user.id < rank_save[tie_i+j].user.id {
                                    let r1 = rank_save[tie_i+j].clone();
                                    let r2 = rank_save[tie_i+j+1].clone();
                                    rank_save[tie_i+j] = r2;
                                    rank_save[tie_i+j+1] = r1;
                                }
                            }
                        }
                    } else if rank_arg.tie_breaker.as_ref().unwrap() == "submission_count" {
                        if rank_save[tie_i+j+1].sub_count < rank_save[tie_i+j].sub_count {
                            let r1 = rank_save[tie_i+j].clone();
                            let r2 = rank_save[tie_i+j+1].clone();
                            rank_save[tie_i+j] = r2;
                            rank_save[tie_i+j+1] = r1;
                        } else if rank_save[tie_i+j+1].sub_count == rank_save[tie_i+j].sub_count {
                            if rank_save[tie_i+j+1].user.id < rank_save[tie_i+j].user.id {
                                let r1 = rank_save[tie_i+j].clone();
                                let r2 = rank_save[tie_i+j+1].clone();
                                rank_save[tie_i+j] = r2;
                                rank_save[tie_i+j+1] = r1;
                            }
                        }
                    } else {
                        return HttpResponse::BadRequest().json( MyError{ code: 1, reason: "ERR_INVALID_ARGUMENT".to_string(), message: "Invalid argument tie_breaker".to_string() } );
                    }
                }
            }
            tie_end -= 1;
        }
        tie_i = tie_end + 1;
    }
    //change the tmp_rank into rank list to output
    let mut rank: Vec<Rank> = Vec::new();
    rank.push( Rank{ user: rank_save[0].user.clone(), rank: 1, scores: Vec::new() } );
    for i in rank_save[0].jobs.clone() {
        if i.is_none() {
            rank[0].scores.push(0.0);
        } else {
            rank[0].scores.push(i.unwrap().score.clone());
        }
    }
    let mut rank_now: usize = 1;
    for i in 1..rank_save.len() {
        if rank_save[i].score < rank_save[i-1].score {
            rank_now = i+1;
        } else {
            if rank_arg.tie_breaker.as_ref().is_some() {
                if rank_arg.tie_breaker.as_ref().unwrap() == "user_id" {
                    rank_now = i+1;
                } else if rank_arg.tie_breaker.as_ref().unwrap() == "submission_time" && rank_save[i].sub_time != rank_save[i-1].sub_time {
                    rank_now = i+1;
                } else if rank_arg.tie_breaker.as_ref().unwrap() == "submission_count" && rank_save[i].sub_count != rank_save[i-1].sub_count {
                    rank_now = i+1;
                }
            }
        }
        rank.push( Rank{ user: rank_save[i].user.clone(), rank: rank_now, scores: Vec::new() } );
        for j in rank_save[i].jobs.clone() {
            if j.is_none() {
                rank[i].scores.push(0.0);
            } else {
                rank[i].scores.push(j.unwrap().score.clone());
            }
        }
    }
    HttpResponse::Ok().json(rank.clone())
}
//basic part end

//database impliment part
//the basic logic used in this part is to save the global variables
//in database and global at the same time.
//there's no complex table,
//data saved as string that can be parsed to json structure in one table line.
//when we want to use the data, 
//just convert the hole string into a structure.
//This makes the database easy to build, write and read by program.
//this used to save data contents temporarily
#[derive(Clone)]
struct Data (String);

//create a new database with initial datas
fn create_database()-> Result<(),rusqlite::Error> {
    let database = Connection::open("data.db")?;
    database.execute("CREATE TABLE users (
        id   INTEGER PRIMARY KEY,
        contents TEXT NOT NULL
    )", [])?;
    database.execute("CREATE TABLE jobs (
        id   INTEGER PRIMARY KEY,
        contents TEXT NOT NULL
    )", [])?;
    database.execute("CREATE TABLE contests (
        id   INTEGER PRIMARY KEY,
        contents TEXT NOT NULL,
        subn TEXT
    )", [])?;
    data_insert("users".to_string(), to_string_pretty(&User{ id : 0, name : "root".to_string() }).unwrap())?;
    let mut save_sub: Vec<(usize, usize)> = Vec::new();
    save_sub.push((0 as usize, 0 as usize));
    data_insert("contests".to_string(), to_string_pretty(&Contest{ id: 0, name: String::new(), from: String::new(), to: String::new(), problem_ids: Vec::new(), user_ids: Vec::new(), submission_limit: 0 }).unwrap())?;
    data_update_for_subn(0, to_string_pretty(&save_sub).unwrap())?;
    Ok(())
}

//load jobs, users and contest saved in database
fn load_data()-> Result<(),rusqlite::Error> {
    let database = Connection::open("data.db")?;
    let mut get_users_from_db = database.prepare("SELECT contents FROM users")?;
    let get_users_iter = get_users_from_db.query_map([], |row| {Ok(Data(row.get(0)?))})?;
    for i in get_users_iter {
        USERS_LIST.lock().unwrap().push(serde_json::from_value::<User>(serde_json::from_str(&i.unwrap().0).unwrap()).unwrap());
    }
    let mut get_jobs_from_db = database.prepare("SELECT contents FROM jobs")?;
    let get_jobs_iter = get_jobs_from_db.query_map([], |row| {Ok(Data(row.get(0)?))})?;
    for i in get_jobs_iter {
        JOB_LIST.lock().unwrap().push(serde_json::from_value::<Job>(serde_json::from_str(&i.unwrap().0).unwrap()).unwrap());
    }
    let mut get_contests_from_db = database.prepare("SELECT contents, subn FROM contests")?;
    let get_contests_iter = get_contests_from_db.query_map([], |row| {Ok((Data(row.get(0)?), Data(row.get(1)?)))})?;
    for _i in get_contests_iter {
        let i = _i?; 
        CONTESTS_LIST.lock().unwrap().push(serde_json::from_value::<Contest>(serde_json::from_str(&i.clone().0.0).unwrap()).unwrap());
        CONTESTS_SUB_LIMIT.lock().unwrap().push(serde_json::from_value::<Vec<(usize,usize)>>(serde_json::from_str(&i.clone().1.0).unwrap()).unwrap());
    }
    Ok(())
}

//update contents in line {id} in {table} to {contents}
fn data_update(table: String, id: usize, contents: String)-> Result<(),rusqlite::Error> {
    let database = Connection::open("data.db")?;
    database.execute(&format!("UPDATE {} SET contents = '{}' WHERE id = {}",table,contents,id+1), [])?;
    Ok(())
}

//update submission amount especailly
fn data_update_for_subn(id: usize, subn: String)-> Result<(),rusqlite::Error> {
    let database = Connection::open("data.db")?;
    database.execute(&format!("UPDATE contests SET subn = '{}' WHERE id = {}",subn ,id+1), [])?;
    Ok(())
}

//new a line in {table} with {contents}
fn data_insert(table: String, contents: String)-> Result<(),rusqlite::Error> {
    let database = Connection::open("data.db")?;
    database.execute(&format!("INSERT INTO {} (contents) VALUES (?)",table), params![&contents])?;
    Ok(())
}
//database part end


// DO NOT REMOVE: used in automatic testing
#[post("/internal/exit")]
#[allow(unreachable_code)]
async fn exit() -> impl Responder {
    log::info!("Shutdown as requested");
    std::process::exit(0);
    format!("Exited")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    //init
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let _ = std::fs::remove_dir_all("tmp");
    *ARGL.lock().unwrap() = Argu::parse();
    if ARGL.lock().unwrap().config.as_ref().is_none() {
        ARGL.lock().unwrap().config = Some("config.json".to_string());
    }
    let config = serde_json::from_value::<Config>(
        serde_json::from_str(
          &std::fs::read_to_string(ARGL.lock().unwrap().config.as_ref().unwrap().clone())?)?)?;
    
    if ARGL.lock().unwrap().flush_data {
        let _ = std::fs::remove_file("data.db");
        let _ = create_database();
    }
    let _ = load_data();
    let mut server_address = "127.0.0.1".to_string();
    let mut server_port = 12345;
    if config.server.bind_address.is_some() { server_address = config.server.bind_address.clone().unwrap(); }
    if config.server.bind_port.is_some() { server_port = config.server.bind_port.clone().unwrap(); }
    //start server
    HttpServer::new(move || {
        //App::new().app_data(web::Data::new(config.clone()));
        App::new()
            .app_data(web::Data::new(config.clone()))
            .wrap(Logger::default())
            .service(post_jobs)
            .service(get_jobs)
            .service(put_jobs)
            .service(get_jobs_from_id)
            .service(get_users)
            .service(post_users)
            .service(greet)
            .service(post_contests)
            .service(get_contests)
            .service(get_contests_from_id)
            .service(get_rank)
            // DO NOT REMOVE: used in automatic testing
            .service(exit)
    })
    .bind((server_address, server_port))?
    .run()
    .await

}
