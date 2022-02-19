use clap::{AppSettings, Args, Parser};
use ffmpeg_next as ffmpeg;
use ffmpeg_next::media::Type;
use macros::*;
use rand::seq::SliceRandom;
use rusqlite::OptionalExtension;
use std::collections::HashSet;
use std::str::FromStr;

#[derive(Parser)]
#[clap(setting(AppSettings::DisableHelpSubcommand))]
#[clap(setting(AppSettings::DeriveDisplayOrder))]
struct Arg {
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    #[clap(about = "Add the files in './pending/' into management")]
    Add,

    #[clap(about = "Tag the video with given word")]
    Tag {
        #[clap(help = "The video's name (extension part and dash character removed name)")]
        name: String,
        #[clap(help = "The word to tag the video")]
        tag: String,
    },

    #[clap(about = "List the video(s) filtered by given condition(s)")]
    List {
        #[clap(flatten)]
        arg: FilterArg,

        #[clap(short, long)]
        #[clap(help = "Show verbose information")]
        verbose: bool,

        #[clap(long)]
        #[clap(help = "Make symlink(s) from the filtered video(s) into directory './links/'")]
        link: bool
    },

    #[clap(about = "Clean './links/'")]
    Clean,

    #[clap(about = "Check the inconsistency between file system and database")]
    Check {
        #[clap(short, long)]
        #[clap(help = "Fix the inconsistency between file system and database")]
        fix: bool,
    },
}

#[derive(Args)]
#[clap(setting(AppSettings::DeriveDisplayOrder))]
struct FilterArg {
    #[clap(short, long)]
    #[clap(help = "The name(s) of filtered video(s) should start with <NAME>")]
    name: Option<String>,

    #[clap(short, long)]
    #[clap(help = "The filtered video(s) should be tagged by <TAG>")]
    tag: Option<String>,

    #[clap(short, long)]
    #[clap(
        help = "The duration of filtered video(s) should equal approximately to <LENGTH>; Accept formats like 'S', 'M:S', 'H:M:S'"
    )]
    duration: Option<DurationArg>,

    #[clap(short = 'e', long)]
    #[clap(default_value = "5")]
    #[clap(help = "The error in second(s) when filtering videos by duration")]
    duration_range: usize,

    #[clap(short, long)]
    #[clap(default_value = "0")]
    #[clap(help = "Limit the total number of filtered video(s); 0 means no limit")]
    limit: usize,
}

struct DurationArg(usize);
impl FromStr for DurationArg {
    type Err = <usize as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(':').rev();
        let mut seconds = 0;
        let mut unit = 1;
        for p in parts {
            let n: usize = p.parse()?;
            seconds += n * unit;
            unit *= 60;
        }
        Ok(Self(seconds))
    }
}

#[derive(Default, CreateTable, CreateIndex, Select, Insert)]
#[table_name(video)]
struct VideoEntry {
    #[primary]
    name: String,
    #[index]
    tag: String,
    file_name: String,
    file_size: u32,
    #[index]
    duration: u32,
    video_codec: String,
    video_bit_rate: u32,
    video_frame_rate: u32,
    video_width: u32,
    video_height: u32,
    audio_codec: String,
    audio_bit_rate: u32,
}

#[derive(Default, Select)]
#[table_name(video)]
struct BriefVideoEntry {
    name: String,
    tag: String,
    file_name: String,
    file_size: u32,
    duration: u32,
}

static mut DB_CONNECTION: Option<rusqlite::Connection> = None;

fn main() {
    let arg = Arg::parse();
    match arg.command {
        Command::Add => do_add(),
        Command::Tag { name, tag } => do_tag(name, tag),
        Command::List { arg, verbose, link } => do_list(arg, verbose, link),
        Command::Clean => do_clean(),
        Command::Check { fix } => do_check(fix),
    }
}

#[inline]
fn db_connection() -> &'static rusqlite::Connection {
    unsafe { DB_CONNECTION.as_ref().unwrap() }
}

fn prepare_environments() {
    let path = std::env::current_exe().unwrap();
    let path = path.parent().unwrap();
    std::env::set_current_dir(path).unwrap();

    prepare_directory("files");
    prepare_directory("links");
    prepare_directory("pending");
    let db_connection = prepare_database();
    unsafe { DB_CONNECTION = Some(db_connection) };
}

fn prepare_directory(dir_name: &str) {
    match std::fs::metadata(dir_name) {
        Ok(md) => {
            if !md.is_dir() {
                panic!("'./{dir_name}' exists but is not a directory")
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(dir_name).unwrap();
        }
        Err(e) => {
            panic!("failed to prepare directory: {e}");
        }
    };
}

fn prepare_database() -> rusqlite::Connection {
    let c = rusqlite::Connection::open("database").unwrap();
    VideoEntry::create_table(&c).unwrap();
    VideoEntry::create_indexes(&c).unwrap();
    c
}

fn prepare_where_clause(arg: &FilterArg) -> (String, Vec<String>) {
    let mut exprs = vec![];
    let mut params = vec![];

    if let Some(name) = &arg.name {
        let name = name.to_ascii_uppercase();
        exprs.push("name GLOB ?");
        params.push(format!("{name}*"));
    }
    if let Some(tag) = &arg.tag {
        let tag = tag.to_ascii_uppercase();
        exprs.push("tag=?");
        params.push(tag.clone());
    }
    if let Some(duration) = &arg.duration {
        let min = usize::saturating_sub(duration.0, arg.duration_range);
        let max = usize::saturating_add(duration.0, arg.duration_range);
        exprs.push("duration BETWEEN ? AND ?");
        params.push(min.to_string());
        params.push(max.to_string());
    }

    let clause = if exprs.is_empty() {
        String::from("ORDER BY name")
    } else {
        let mut s = String::from("WHERE ");
        s.push_str(&exprs.join(" AND "));
        s.push_str(" ORDER BY name");
        s
    };
    (clause, params)
}

fn readable_file_size(file_size: u32) -> String {
    let mut size = file_size;
    let mut unit = "B";
    let mut unit_changed = false;
    if size > 1024 {
        unit_changed = true;
        size /= 1024;
        unit = "KB";
        if size > 1024 {
            size /= 1024;
            unit = "MB";
            if size > 1024 {
                size /= 1024;
                unit = "GB";
            }
        }
    }
    if unit_changed {
        format!("{size}{unit}({file_size})")
    } else {
        format!("{size}{unit}")
    }
}

fn readable_duration(duration: u32) -> String {
    let mut seconds = duration;
    let mut minutes = 0;
    let mut hours = 0;
    if seconds >= 60 {
        minutes = seconds / 60;
        seconds %= 60;
    }
    if minutes >= 60 {
        hours = minutes / 60;
        minutes %= 60;
    }
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn clear_directory(dir: &str) -> std::io::Result<()> {
    let read_dir = std::fs::read_dir(dir)?;
    for entry in read_dir {
        let entry = entry?;
        let md = entry.metadata()?;
        if md.is_symlink() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            std::fs::remove_file(format!("{dir}/{name}"))?;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn make_link(file_name: &str) -> std::io::Result<()> {
    let mut src = std::env::current_dir().unwrap();
    let mut dst = src.clone();
    src.push("files");
    src.push(file_name);
    dst.push("links");
    dst.push(file_name);
    std::os::windows::fs::symlink_file(src, dst)
}

#[cfg(not(windows))]
fn make_link(file_name: &str) -> std::io::Result<()> {
    let mut src = std::env::current_dir().unwrap();
    let mut dst = src.clone();
    src.push("files");
    src.push(file_name);
    dst.push("links");
    dst.push(file_name);
    std::os::unix::fs::symlink(src, dst)
}

fn do_add() {
    prepare_environments();
    match std::fs::read_dir("pending") {
        Ok(entries) => {
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                do_add_file("pending", &file_name);
            }
        }
        Err(e) => {
            eprintln!("failed to read directory: {e}");
        }
    }
}

fn do_add_file(dir: &str, file_name: &str) {
    let md = match std::fs::metadata(format!("{dir}/{file_name}")) {
        Ok(md) => md,
        Err(e) => {
            eprintln!("failed to read file: {e}");
            return;
        }
    };
    if !md.is_file() {
        println!("skip non-file '{file_name}'");
        return;
    }
    let file_size = md.len();
    let base_name = file_name.split('.').next().unwrap();
    let name: String = base_name
        .chars()
        .filter(|c| *c != '-')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    let result: Result<Option<u32>, _> = db_connection()
        .query_row(
            "SELECT 1 FROM video WHERE name=?",
            rusqlite::params![&name],
            |row| row.get(0),
        )
        .optional();
    match result {
        Ok(opt) => {
            if opt.is_some() {
                eprintln!(
                    "skip file '{file_name}'; an entry with name '{name}' already existed in database"
                );
                return;
            }
        }
        Err(e) => {
            eprintln!("failed to query database: {e}");
            return;
        }
    }

    let mut entry = VideoEntry {
        name,
        file_name: file_name.to_string(),
        file_size: file_size as _,
        ..VideoEntry::default()
    };
    match ffmpeg::format::input(&format!("{dir}/{file_name}")) {
        Ok(input) => {
            if input.duration() <= 0 {
                eprintln!("skip file '{file_name}'; its duration is not a positive number");
                return;
            }
            entry.duration = (input.duration() as f64 / ffmpeg::ffi::AV_TIME_BASE as f64) as _;

            let mut video_stream_read = false;
            let mut audio_stream_read = false;
            for stream in input.streams() {
                let codec = stream.codec();
                match codec.medium() {
                    Type::Video => {
                        if video_stream_read {
                            eprintln!("skip file '{file_name}'; multiple video streams found");
                            continue;
                        }
                        video_stream_read = true;
                        entry.video_codec = codec.id().name().to_string();
                        let video = match codec.decoder().video() {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("skip file '{file_name}'; failed to read video stream info: {e}");
                                continue;
                            }
                        };
                        entry.video_bit_rate = (video.bit_rate() as f64 / 1000f64) as _;
                        entry.video_frame_rate =
                            (stream.rate().0 as f64 / stream.rate().1 as f64) as _;
                        entry.video_width = video.width();
                        entry.video_height = video.height();
                    }
                    Type::Audio => {
                        if audio_stream_read {
                            eprintln!("skip file '{file_name}'; multiple audio streams found");
                            continue;
                        }
                        audio_stream_read = true;
                        entry.audio_codec = codec.id().name().to_string();
                        let audio = match codec.decoder().audio() {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!("skip file '{file_name}'; failed to read audio stream info: {e}");
                                continue;
                            }
                        };
                        entry.audio_bit_rate = (audio.bit_rate() as f64 / 1000f64) as _;
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {
            eprintln!("skip file '{file_name}'; failed to read file: {e}");
            return;
        }
    }

    println!("add file '{file_name}' as '{}':", entry.name);
    println!("  file_size={}", readable_file_size(entry.file_size));
    println!("  duration={}", readable_duration(entry.duration));
    println!("  video_codec={}", entry.video_codec);
    println!("  video_bit_rate={}kbps", entry.video_bit_rate);
    println!("  video_frame_rate={}fps", entry.video_frame_rate);
    println!("  video_width={}px", entry.video_width);
    println!("  video_height={}px", entry.video_height);
    println!("  audio_codec={}", entry.audio_codec);
    println!("  audio_bit_rate={}kbps", entry.audio_bit_rate);
    match entry.insert(db_connection()) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("skip file '{file_name}'; failed to insert entry: {e}");
            return;
        }
    }

    if dir != "files" {
        match std::fs::rename(format!("{dir}/{file_name}"), format!("files/{file_name}")) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("failed to move file '{file_name}': {e}");
            }
        }
    }
}

fn do_tag(name: String, tag: String) {
    prepare_environments();
    let name = name.to_ascii_uppercase();
    let tag = tag.to_ascii_uppercase();
    match db_connection().execute(
        "UPDATE video SET tag=? WHERE name=?",
        rusqlite::params![&tag, &name],
    ) {
        Ok(n) => {
            if n > 0 {
                println!("'{name}' is tagged with '{tag}'");
            } else {
                eprintln!("failed to tag '{name}' with '{tag}', entry not found");
            }
        }
        Err(e) => {
            eprintln!("failed to tag '{name}' with '{tag}': {e}");
        }
    }
}

fn do_list(filter_arg: FilterArg, verbose: bool, link: bool) {
    prepare_environments();
    let (where_clause, where_params) = prepare_where_clause(&filter_arg);
    let where_params: Vec<&dyn rusqlite::ToSql> = where_params
        .iter()
        .map(|s| s as &dyn rusqlite::ToSql)
        .collect();
    let where_params = where_params.as_slice();
    if verbose {
        do_list_verbosely(&where_clause, where_params, filter_arg.limit, link);
    } else {
        do_list_briefly(&where_clause, where_params, filter_arg.limit, link);
    }
}

fn do_list_verbosely<P: rusqlite::Params>(where_clause: &str, params: P, limit: usize, link: bool) {
    let mut entries: Vec<VideoEntry> =
        match VideoEntry::select(db_connection(), where_clause, params) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("failed to query database: {}", e);
                return;
            }
        };
    if limit > 0 {
        let mut rng = rand::thread_rng();
        entries.shuffle(&mut rng);
        entries.truncate(limit);
        entries.sort_by(|a, b| a.name.cmp(&b.name));
    }
    for entry in &entries {
        let name = &entry.name;
        let tag = &entry.tag;
        let file_name = &entry.file_name;
        let file_size = readable_file_size(entry.file_size);
        let duration = readable_duration(entry.duration);
        let video_codec = &entry.video_codec;
        let video_bit_rate = entry.video_bit_rate;
        let video_frame_rate = entry.video_frame_rate;
        let video_width = entry.video_width;
        let video_height = entry.video_height;
        let audio_codec = &entry.audio_codec;
        let audio_bit_rate = entry.audio_bit_rate;
        println!("{name}[{tag}] {duration} {file_name}/{file_size}");
        println!("  video: codec={video_codec}, bit_rate={video_bit_rate}kbps, frame_rate={video_frame_rate}fps, resolution={video_width}x{video_height}");
        println!("  audio: codec={audio_codec}, bit_rate={audio_bit_rate}kbps");
    }
    if !entries.is_empty() && link {
        match clear_directory("links") {
            Ok(_) => {}
            Err(e) => {
                eprintln!("failed to clear directory './links/': {e}");
                return;
            }
        }
        for entry in &entries {
            let file_name = &entry.file_name;
            match make_link(&file_name) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("failed to make link for '{file_name}': {e}")
                }
            }
        }
    }
}

fn do_list_briefly<P: rusqlite::Params>(where_clause: &str, params: P, limit: usize, link: bool) {
    let mut entries: Vec<BriefVideoEntry> =
        match BriefVideoEntry::select(db_connection(), where_clause, params) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("failed to query database: {}", e);
                return;
            }
        };
    if limit > 0 {
        let mut rng = rand::thread_rng();
        entries.shuffle(&mut rng);
        entries.truncate(limit);
        entries.sort_by(|a, b| a.name.cmp(&b.name));
    }
    for entry in &entries {
        let name = &entry.name;
        let tag = &entry.tag;
        let file_name = &entry.file_name;
        let file_size = readable_file_size(entry.file_size);
        let duration = readable_duration(entry.duration);
        println!("{name}[{tag}] {duration} {file_name}/{file_size}");
    }
    if !entries.is_empty() && link {
        match clear_directory("links") {
            Ok(_) => {}
            Err(e) => {
                eprintln!("failed to clear directory './links/': {e}");
                return;
            }
        }
        for entry in &entries {
            let file_name = &entry.file_name;
            match make_link(&file_name) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("failed to make link for '{file_name}': {e}")
                }
            }
        }
    }
}

fn do_clean() {
    match clear_directory("links") {
        Ok(_) => {}
        Err(e) => {
            eprintln!("failed to clear directory './links/': {e}");
        }
    }
}

fn do_check(fix: bool) {
    prepare_environments();

    let mut db_file_names: HashSet<String> = HashSet::new();
    let mut stmt = db_connection()
        .prepare("SELECT file_name FROM video")
        .unwrap();
    let rows = match stmt.query_map([], |row| {
        let s: String = row.get(0)?;
        Ok(s)
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to query database: {}", e);
            return;
        }
    };
    for row in rows {
        match row {
            Ok(s) => {
                db_file_names.insert(s);
            }
            Err(e) => {
                eprintln!("failed to query database: {}", e);
                return;
            }
        }
    }

    let mut fs_file_names = HashSet::new();
    for entry in std::fs::read_dir("files").unwrap() {
        let entry = entry.unwrap();
        fs_file_names.insert(entry.file_name().to_string_lossy().to_string());
    }

    for name in db_file_names.difference(&fs_file_names) {
        println!("'{name}' exists in database, but not in file system");
        if fix {
            match db_connection().execute(
                "DELETE FROM video WHERE file_name=?",
                rusqlite::params![name],
            ) {
                Ok(_) => {
                    println!("invalid entry '{name}' removed");
                }
                Err(e) => {
                    eprintln!("failed to remove invalid entry '{name}': {e}");
                }
            }
        }
    }

    for name in fs_file_names.difference(&db_file_names) {
        println!("'{name}' exists in file system, but not in database");
        if fix {
            do_add_file("files", name);
        }
    }
}
