# RustiqueDB
"Rust" and "Antique", a classic yet modern tool.

## Project Structure
```
RustiqueDB/
├── Cargo.toml          # Rust项目配置文件
├── src/
│   ├── lib.rs          # 核心库模块
│   ├── database/       # 数据库核心实现
│   │   └── mod.rs      # 表/列/数据类型定义和操作
│   ├── main.rs         # 通过终端运行的入口
│   ├── parser.rs       # SQL 解析逻辑
│   ├── format.rs       # 表格格式化输出
│   ├── error.rs        # 错误处理模块
├── data/               # JSON数据存储目录
├── examples/           # 示例代码
│   ├── order_test.rs   # 排序功能演示
│   ├── where_test.rs   # 条件查询演示
├── tests/              # 测试模块
```

## Key Features
✅ 完整CRUD操作支持  
✅ 多条件WHERE筛选  
✅ 多列ORDER BY排序  
✅ 持久化存储(JSON)  
✅ 格式化表格输出  

## Usage
```bash
# 运行示例
cargo run --example order_test
cargo run --example where_test

# 生成文档
cargo doc --open
```

## Roadmap
- [ ] UPDATE/DELETE操作
- [ ] SELECT计算表达式
- [ ] 多行SQL支持
- [ ] 特色功能

测试样例出现的错误

DROP 需支持多个表同时丢弃 样例8 (true)
eg:DROP TABLE movies_test8, movies2_test8;

DELETE 好像字符串匹配有问题 样例10
eg:DELETE FROM books_test10 WHERE discription="A book for rust development";

SELECT 需要支持and 样例11
eg:SELECT name, price*2 FROM books_test11 where id < 3 and id > 1;
SELECT name, price FROM books_test11 where id < 3 and id > 1;

样例12 表达式


样例13 
INSERT INTO books_test13 (id, name, price) VALUES (2, "Rust Porgraming", 66);

