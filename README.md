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
│   ├── bin/            # 可执行文件
│   │   └── rustique_db.rs # 主程序入口
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
