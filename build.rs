use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");

    let output = Command::new("git")
        .args(&["describe", "--tags", "--always"])
        .output()
        .expect("Failed to execute git command");

    let git_hash = String::from_utf8(output.stdout)
        .expect("Invalid UTF-8 from git output")
        .trim()
        .to_string();

    println!("cargo:rustc-env=VERSION={}", git_hash);
}
