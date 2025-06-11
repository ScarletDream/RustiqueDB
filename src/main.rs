use std::io::{self, Write};
use rustique_db::database::{Database, DataType};
use rustique_db::format::format_table;
use rustique_db::format::format_table_from_db;
use rustique_db::parser::{parse_sql, SqlAst};
use rustique_db::history::CommandHistory;
use rustique_db::execute_sql;

// 注释处理
fn remove_comments(input: &str) -> &str {
    let mut in_block_comment = false;
    let mut in_line_comment = false;
    let mut last_valid_pos = 0;
    let bytes = input.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        match (b, in_block_comment, in_line_comment) {
            // 检测块注释开始
            (b'/', _, false) if i+1 < bytes.len() && bytes[i+1] == b'*' => {
                in_block_comment = true;
            },
            // 检测块注释结束
            (b'*', true, _) if i+1 < bytes.len() && bytes[i+1] == b'/' => {
                in_block_comment = false;
            },
            // 检测行注释开始
            (b'-', false, false) if i+1 < bytes.len() && bytes[i+1] == b'-' => {
                in_line_comment = true;
            },
            // 处理换行符（行注释结束）
            (b'\n', _, true) => {
                in_line_comment = false;
                last_valid_pos = i + 1; // 保留换行符保证行号正确
            },
            // 有效字符处理
            (_, false, false) => {
                last_valid_pos = i + 1;
            },
            _ => {}
        }
    }

    // 返回原始输入的切片引用（零拷贝）
    &input[..last_valid_pos]
}

// 带历史支持的输入读取
fn read_input_with_history(prompt: &str, history: &mut CommandHistory) -> String {
    let mut input = String::new();
    let mut is_multiline = false;

    loop {
        print!("{}", if is_multiline { "...> " } else { prompt });
        io::stdout().flush().unwrap();

        let mut line = String::new();
        io::stdin().read_line(&mut line).unwrap();

        // 处理历史命令导航（仅在第一行）
        if !is_multiline {
            match line.trim_end() {
                "\x1b[A" => { // 上箭头
                    if let Some(cmd) = history.get_previous() {
                        input = cmd.to_string();
                        print!("\r\x1b[K{}{}", prompt, input);
                        continue;
                    }
                }
                "\x1b[B" => { // 下箭头
                    if let Some(cmd) = history.get_next() {
                        input = cmd.to_string();
                        print!("\r\x1b[K{}{}", prompt, input);
                        continue;
                    }
                }
                _ => {}
            }
        }

        input.push_str(&line);

        // 检查结束条件（分号或exit）
        let trimmed = input.trim();
        if trimmed.ends_with(';') || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        is_multiline = true;
    }

    input.trim().to_string()
}

fn should_exit(input: &str) -> bool {
    let trimmed = input.trim().to_lowercase();
    trimmed == "exit" || trimmed == "exit;" || trimmed == "quit" || trimmed == "quit;"
}

fn clean_command_arg(input: &str) -> &str {
    input.trim().trim_end_matches(';').trim()
}

fn main() {
    let mut history = CommandHistory::new(100);
    let mut db = Database::load_with_history(&mut history).unwrap_or_else(|_| {
        println!("Creating new database...");
        Database::new()
    });

    println!("Welcome to RustiqueDB!");
    println!("Database loaded with {} tables", db.tables.len());
    
    println!("Enter SQL commands (type 'exit' to quit, use ; to end commands):");

    println!("Special commands:");
    println!("  !!;       - 重复上一条命令");
    println!("  !n;       - 执行历史记录中第n条命令");
    println!("  HISTORY;  - 显示所有历史命令");
    println!("  CLEAR;    - 清空历史记录");
    
    loop {
        let input = read_input_with_history("sql> ", &mut history);

        if should_exit(&input) {
            if let Err(e) = db.save() {
                eprintln!("Failed to save database: {}", e);
            }
            println!("Goodbye!");
            break;
        }

        let trimmed = input.trim();

        // 特殊命令处理
        match trimmed {
            "HISTORY" | "HISTORY;" => {
                for (i, cmd) in history.enumerate() {
                    println!("{:4}: {}", i, cmd.trim());
                }
                continue;
            },
            "CLEAR" | "CLEAR;" => {
                history.clear();
                println!("Command history cleared");
                continue;
            },
            "!!" | "!!;" => {
                if let Some(last) = history.get_full_command(history.len().saturating_sub(1)) {
                    println!("Re-executing: {}", last.trim());
                    let _ = execute_sql(&last, &mut db, &mut history);
                }
                continue;
            },
            cmd if cmd.starts_with('!') => {
                let arg = clean_command_arg(&cmd[1..]); // 清理参数
                if let Ok(n) = arg.parse::<usize>() {
                    if let Some(cmd) = history.get_full_command(n) {
                        println!("Executing #{}: {}", n, cmd.trim());
                        let _ = execute_sql(&cmd, &mut db, &mut history);
                    } else {
                        eprintln!("Error: No history entry at index {}", n);
                    }
                } else {
                    eprintln!("Error: Invalid history index '{}'", arg);
                }
                continue;
            },
            _ => {}
        }

        if !trimmed.is_empty() {
            history.add(&input);
            let _ = execute_sql(trimmed, &mut db, &mut history);
        }
    }
}
