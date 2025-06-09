use std::env;
use std::fs;
use simple_db::execute_sql;

fn main() {
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();
    
    // 检查参数数量
    if args.len() != 2 {
        eprintln!("Usage: {} <sql_file_path>", args[0]);
        std::process::exit(1);
    }

    // 读取SQL文件内容
    let sql_content = match fs::read_to_string(&args[1]) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file: {}", e);
            std::process::exit(1);
        }
    };

    // 执行SQL并输出结果
    let success = execute_sql(&sql_content);
    if !success {
        std::process::exit(1);
    }
}
