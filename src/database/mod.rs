use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

// 为所有需要序列化的类型添加derive
#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub data: Vec<Vec<String>>,  // Vec<String> 本身是可序列化的
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataType {
    Int(u32),
    Varchar(u32),
}

#[derive(Debug, Serialize, Deserialize,Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary: bool,
    pub not_null: bool,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    // 创建表方法
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<(&str, DataType, bool, bool)>, // (列名, 类型, 是否主键, 是否非空)
    )-> Result<(), String>{

        let normalized_name = name.trim().to_lowercase();
        
        // 原子化检查-创建操作
        let exists = self.tables.iter().any(|t| t.name.to_lowercase() == normalized_name);
        if exists {
            return Err(format!("[REJECTED] Table '{}' exists", normalized_name)); // 确保此返回不可跳过
        }
        self.tables.push(Table {
            name: name.to_string(),
            columns: columns
                .into_iter()
                .map(|(name, data_type, is_primary, not_null)| Column {
                    name: name.to_string(),
                    data_type,
                    is_primary,
                    not_null,
                })
                .collect(),
            data: Vec::new(),
        });
        Ok(())
    }

    // 数据插入方法
    pub fn insert(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>, // 新增：可选列名列表
        values: Vec<Vec<&str>>,
    ) -> Result<usize, String> {
        let table = self.tables.iter_mut()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        let mut inserted_rows = 0;

        for row_values in values {
            // 处理部分插入
            let full_row_values = if let Some(col_names) = &columns {
                // 创建完整行数据，未指定的列设为空字符串
                let mut full_row = vec![""; table.columns.len()];
                
                // 检查列名是否匹配
                if col_names.len() != row_values.len() {
                    return Err("Column count mismatch in INSERT statement".into());
                }
                
                for (i, col_name) in col_names.iter().enumerate() {
                    let col_index = table.columns.iter()
                        .position(|c| &c.name == col_name)
                        .ok_or(format!("Column '{}' not found", col_name))?;
                    
                    full_row[col_index] = row_values[i];
                }
                
                full_row
            } else {
                // 全列插入
                if row_values.len() != table.columns.len() {
                    return Err("Column count mismatch".into());
                }
                row_values
            };

            // 检查NOT NULL约束和主键
            for (i, (value, column)) in full_row_values.iter().zip(&table.columns).enumerate() {
                let is_null = value.trim().is_empty() || value.trim().eq_ignore_ascii_case("null");
                
                if column.not_null && is_null {
                    return Err(format!("Error: Field '{}' doesn't have a default value", column.name));
                }

                if column.is_primary && is_null {
                    return Err(format!("Error: Field '{}' doesn't have a default value", column.name));
                }
            }

            // 主键唯一性检查
            if let Some(pk_index) = table.columns.iter().position(|c| c.is_primary) {
                let pk_value = full_row_values[pk_index];
                if !pk_value.trim().is_empty() && !pk_value.trim().eq_ignore_ascii_case("null") {
                    if table.data.iter().any(|row| row[pk_index] == pk_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", pk_value));
                    }
                }
            }

            let row: Vec<String> = full_row_values.iter().map(|s| {
                if s.trim().eq_ignore_ascii_case("null") {
                    String::new()
                } else {
                    s.to_string()
                }
            }).collect();
            
            table.data.push(row);
            inserted_rows += 1;
        }

        Ok(inserted_rows)
    }

    pub fn update(
        &mut self,
        table_name: &str,
        set: Vec<(String, String)>,
        condition: Option<&str>,
    ) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = self.tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        // 2. 提前收集所有需要的列信息
        let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
        let not_null_flags: Vec<bool> = table.columns.iter().map(|c| c.not_null).collect();
        let is_primary_flags: Vec<bool> = table.columns.iter().map(|c| c.is_primary).collect();

        // 3. 创建列名到索引的映射
        let column_map: std::collections::HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();

        // 4. 预处理set值，移除字符串值的引号
        let processed_set: Vec<(String, String)> = set.into_iter()
            .map(|(col_name, value)| {
                // 统一处理值格式，与insert保持一致
                let processed_value = if value.starts_with('"') && value.ends_with('"') {
                    value.trim_matches('"').to_string()
                } else if value.starts_with('\'') && value.ends_with('\'') {
                    value.trim_matches('\'').to_string()
                } else if value.eq_ignore_ascii_case("null") {
                    String::new() // 存储为""表示NULL
                } else {
                    value
                };
                (col_name, processed_value)
            })
            .collect();

        // 5. 检查主键唯一性
        for (col_name, new_value) in &processed_set {
            if let Some(idx) = column_map.get(col_name) {
                if is_primary_flags[*idx] {
                    if table.data.iter().any(|row| &row[*idx] == new_value) {
                        return Err(format!("Duplicate entry '{}' for key 'PRIMARY'", new_value));
                    }
                }
            }
        }

        // 6. 创建过滤闭包
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            let columns = table.columns.clone();
            Box::new(move |row: &[String]| {
                let temp_table = Table {
                    name: String::new(),
                    columns: columns.clone(),
                    data: vec![],
                };
                match Self::parse_condition(cond, &temp_table) {
                    Ok(filter) => filter(row),
                    Err(_) => false,
                }
            })
        } else {
            Box::new(|_| true)
        };

        // 7. 执行更新
        let mut affected_rows = 0;
        for row in &mut table.data {
            if filter_fn(row) {
                affected_rows += 1;
                for (col_name, new_value) in &processed_set {
                    if let Some(idx) = column_map.get(col_name) {
                        // 类型检查
                        match &column_types[*idx] {
                            DataType::Int(_) if new_value.parse::<i32>().is_err() => {
                                return Err(format!("Value '{}' is not INT for column '{}'", 
                                    new_value, col_name));
                            },
                            DataType::Varchar(max_len) if new_value.len() > *max_len as usize => {
                                return Err(format!("Value too long for column '{}' (max {})", 
                                    col_name, max_len));
                            },
                            _ => {}
                        }

                        // 非空检查
                        if not_null_flags[*idx] && new_value.is_empty() {
                            return Err(format!("Column '{}' cannot be null", col_name));
                        }

                        row[*idx] = new_value.clone();
                    }
                }
            }
        }

        Ok(affected_rows)
    }

    pub fn delete(&mut self,table_name: &str,condition: Option<&str>,) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = self.tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        // 2. 提前复制所需的列信息
        let columns = table.columns.clone();

        // 3. 创建过滤闭包
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            // 使用提前复制的列信息
            Box::new(move |row: &[String]| {
                let local_table = Table {
                    name: String::new(),
                    columns: columns.clone(),
                    data: vec![],
                };
                match Self::parse_condition(cond, &local_table) {
                    Ok(filter) => filter(row),
                    Err(_) => false, // 解析失败时不匹配任何行
                }
            })
        } else {
            Box::new(|_| true) // 无条件时匹配所有行
        };

        // 4. 执行删除操作
        let original_len = table.data.len();
        table.data.retain(|row| !filter_fn(row));
        let affected_rows = original_len - table.data.len();

        Ok(affected_rows)
    }

    pub fn save(&self) -> Result<(), String> {
        // 创建data目录（如果不存在）
        fs::create_dir_all("data").map_err(|e| e.to_string())?;

        // 序列化为JSON并保存
        let json = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        fs::write("data/db.json", json).map_err(|e| e.to_string())?;

        Ok(())
    }

    pub fn load() -> Result<Self, String> {
        // 检查文件是否存在
        if !Path::new("data/db.json").exists() {
            return Ok(Database::new());
        }

        // 读取并反序列化
        let json = fs::read_to_string("data/db.json").map_err(|e| e.to_string())?;
        serde_json::from_str(&json).map_err(|e| e.to_string())
    }

    pub fn drop_tables(&mut self, table_names: &[String], if_exists: bool) -> Result<usize, String> {
        let original_count = self.tables.len();
        
        // 只有 if_exists=false 时才检查存在性
        if !if_exists {
            for name in table_names {
                if !self.tables.iter().any(|t| &t.name == name) {
                    return Err(format!("Table '{}' doesn't exist", name));
                }
            }
        }

        // 执行删除（自动跳过不存在的表）
        self.tables.retain(|table| !table_names.contains(&table.name));
        
        let dropped_count = original_count - self.tables.len();
        
        // 如果实际删除数量为0且指定了必须存在，报错
        if dropped_count == 0 && !if_exists {
            return Err("No tables were dropped".into());
        }
        
        Ok(dropped_count)
    }

    pub fn select(
        &self,
        table_name: &str,
        columns: Vec<&str>,
        condition: Option<&str>,
        order_by: Option<Vec<(&str, bool)>>  // (列名, 是否降序)
    ) -> Result<(Vec<Vec<String>>, bool), String> {  // 修改返回值，增加bool表示是否有数据
        let table = self.tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or("Table not found")?;

        // 获取结果列索引
        let column_indices: Vec<usize> = if columns == ["*"] {
            (0..table.columns.len()).collect()
        } else {
            columns.iter().map(|col| {
                table.columns.iter().position(|c| &c.name == col)
                    .ok_or(format!("Column '{}' not found", col))
            }).collect::<Result<_, _>>()?
        };

        // 统一返回 Box<dyn Fn> 类型
        let filter_fn: Box<dyn Fn(&[String]) -> bool> = if let Some(cond) = condition {
            Self::parse_condition(cond, table)?
        } else {
            Box::new(|_| true) // 将闭包装箱
        };

        // 收集原始行数据（带原始行索引）
        let mut rows_with_indices: Vec<(usize, &Vec<String>)> = table.data
            .iter()
            .enumerate()
            .filter(|(_, row)| filter_fn(row))
            .collect();

        // 如果没有匹配的行，直接返回
        if rows_with_indices.is_empty() {
            return Ok((Vec::new(), false));  // 返回空结果和false表示无数据
        }

        // 处理排序（如果需要）
        if let Some(cols) = order_by {
            // 获取排序列元数据
            let sort_specs: Vec<(usize, &DataType, bool)> = cols.into_iter().map(|(col, desc)| {
                let col_idx = table.columns.iter()
                    .position(|c| c.name == col)
                    .ok_or(format!("Sort column '{}' not found", col))?;
                Ok((col_idx, &table.columns[col_idx].data_type, desc))
            }).collect::<Result<_, String>>()?;

            // 排序逻辑（使用原始数据）
            rows_with_indices.sort_by(|(a_idx, _), (b_idx, _)| {
                let a_row = &table.data[*a_idx];
                let b_row = &table.data[*b_idx];

                for (col_idx, data_type, desc) in &sort_specs {
                    let ordering = match data_type {
                        DataType::Int(_) => {
                            a_row[*col_idx].parse::<i32>().unwrap_or(0)
                                .cmp(&b_row[*col_idx].parse::<i32>().unwrap_or(0))
                        },
                        DataType::Varchar(_) => a_row[*col_idx].cmp(&b_row[*col_idx]),
                    };

                    if *desc {
                        return ordering.reverse();
                    } else if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 构建最终结果
        let result = rows_with_indices.into_iter()
            .map(|(_, row)| {
                column_indices.iter().map(|&i| row[i].clone()).collect()
            })
            .collect();

        Ok((result, true))  // 返回结果和true表示有数据
    }


    pub fn parse_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        let cond = cond.trim();
        //println!("[DEBUG parse_condition] 开始解析条件: '{}'", cond);

        // 处理空条件
        if cond.is_empty() {
            return Err("Empty condition".to_string());
        }

        // 1. 处理带引号的字符串中的空格问题
        let mut in_quotes = false;
        let mut modified_cond = String::new();
        for c in cond.chars() {
            match c {
                '"' | '\'' => {
                    in_quotes = !in_quotes;
                    modified_cond.push(c);
                }
                ' ' if in_quotes => modified_cond.push('\u{00A0}'), // 替换为不可见空格
                _ => modified_cond.push(c),
            }
        }

        // 2. 处理括号内的条件
        if modified_cond.starts_with('(') {
            let mut paren_depth = 1;
            let mut end_pos = 1;
            
            while end_pos < modified_cond.len() && paren_depth > 0 {
                match modified_cond.chars().nth(end_pos) {
                    Some('(') => paren_depth += 1,
                    Some(')') => paren_depth -= 1,
                    _ => {}
                }
                end_pos += 1;
            }

            if paren_depth == 0 {
                let inside = modified_cond[1..end_pos-1].replace('\u{00A0}', " ");
                let remaining = modified_cond[end_pos..].replace('\u{00A0}', " ");
                let remaining = remaining.trim();

                //println!("[DEBUG parse_condition] 解析括号内容: '{}', 剩余部分: '{}'", inside, remaining);

                if remaining.is_empty() {
                    return Self::parse_condition(&inside, table);
                } else if remaining.starts_with("AND") || remaining.starts_with("OR") {
                    // 正确处理操作符与括号剩余部分
                    if remaining.starts_with("AND") {
                        let right = remaining[3..].trim();
                        let inside_cond = Self::parse_condition(&inside, table)?;
                        let remaining_cond = Self::parse_condition(right, table)?;
                        return Ok(Box::new(move |row| inside_cond(row) && remaining_cond(row)));
                    } else { // OR
                        let right = remaining[2..].trim();
                        let inside_cond = Self::parse_condition(&inside, table)?;
                        let remaining_cond = Self::parse_condition(right, table)?;
                        return Ok(Box::new(move |row| inside_cond(row) || remaining_cond(row)));
                    }
                } else {
                    // 默认为AND连接
                    let inside_cond = Self::parse_condition(&inside, table)?;
                    let remaining_cond = Self::parse_condition(remaining, table)?;
                    return Ok(Box::new(move |row| inside_cond(row) && remaining_cond(row)));
                }
            }
        }

        // 3. 检查 AND 条件（优先级高于 OR）
        if let Some(pos) = Self::find_outer_operator(&modified_cond, "AND") {
            let left = modified_cond[..pos].trim();
            if left.is_empty() {
                // 如果左边为空，只解析右边
                let right = modified_cond[pos+3..].trim().replace('\u{00A0}', " ");
                return Self::parse_condition(&right, table);
            }
            
            //println!("[DEBUG parse_condition] 发现AND条件，位置: {}", pos);
            let left = left.replace('\u{00A0}', " ");
            let right = modified_cond[pos+3..].trim().replace('\u{00A0}', " ");
            //println!("[DEBUG parse_condition] 分割AND条件: left='{}', right='{}'", left, right);
            
            let left_cond = Self::parse_condition(&left, table)?;
            let right_cond = Self::parse_condition(&right, table)?;
            return Ok(Box::new(move |row| left_cond(row) && right_cond(row)));
        }

        // 4. 检查 OR 条件（优先级低于 AND）
        if let Some(pos) = Self::find_outer_operator(&modified_cond, "OR") {
            let left = modified_cond[..pos].trim();
            if left.is_empty() {
                // 如果左边为空，只解析右边
                let right = modified_cond[pos+2..].trim().replace('\u{00A0}', " ");
                return Self::parse_condition(&right, table);
            }
            
            //println!("[DEBUG parse_condition] 发现OR条件，位置: {}", pos);
            let left = left.replace('\u{00A0}', " ");
            let right = modified_cond[pos+2..].trim().replace('\u{00A0}', " ");
            //println!("[DEBUG parse_condition] 分割OR条件: left='{}', right='{}'", left, right);
            
            let left_cond = Self::parse_condition(&left, table)?;
            let right_cond = Self::parse_condition(&right, table)?;
            return Ok(Box::new(move |row| left_cond(row) || right_cond(row)));
        }

        // 5. 基础条件
        let final_cond = modified_cond.replace('\u{00A0}', " ");
        //println!("[DEBUG parse_condition] 解析基础条件: '{}'", final_cond);
        Self::parse_single_condition(&final_cond, table)
    }

    fn find_outer_operator(s: &str, op: &str) -> Option<usize> {
        let s_lower = s.to_lowercase();
        let op_lower = op.to_lowercase();
        let mut paren_depth = 0;
        let mut in_quotes = false;
        let mut start = 0;

        while let Some(pos) = s_lower[start..].find(&op_lower) {
            let absolute_pos = start + pos;
            let substr = &s[..absolute_pos];
            
            // 检查当前位置是否在括号外且不在引号内
            paren_depth += substr.matches('(').count();
            paren_depth -= substr.matches(')').count();
            in_quotes = substr.matches('"').count() % 2 != 0 || substr.matches('\'').count() % 2 != 0;
            
            if paren_depth == 0 && !in_quotes {
                // 检查是否是完整的操作符（前后有空格或是字符串边界）
                let is_complete = (absolute_pos == 0 || s.as_bytes()[absolute_pos-1].is_ascii_whitespace()) &&
                                 (absolute_pos + op.len() >= s.len() || s.as_bytes()[absolute_pos+op.len()].is_ascii_whitespace());
                
                if is_complete {
                    return Some(absolute_pos);
                }
            }
            
            start = absolute_pos + op.len();
        }
        None
    }

    fn parse_or_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        let orig_cond = cond;
        let cond = cond.trim();
        println!("[DEBUG parse_or_condition] 开始解析条件: '{}'", cond);

        // 1. 先处理最外层的括号
        if cond.starts_with('(') && cond.ends_with(')') {
            println!("[DEBUG parse_or_condition] 去除外层括号: '{}' -> '{}'", cond, &cond[1..cond.len()-1]);
            return Self::parse_or_condition(&cond[1..cond.len()-1], table);
        }

        // 2. 分割条件，处理可能的嵌套情况
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut in_quotes = false;
        let mut paren_depth = 0;
        let mut chars = cond.chars().peekable();
        println!("[DEBUG parse_or_condition] 开始分割条件: '{}'", cond);

        while let Some(c) = chars.next() {
            match c {
                '"' | '\'' => {
                    println!("[DEBUG parse_or_condition] 遇到引号: {}", c);
                    in_quotes = !in_quotes;
                    current_part.push(c);
                }
                '(' if !in_quotes => {
                    paren_depth += 1;
                    println!("[DEBUG parse_or_condition] 进入括号层({}): {}", paren_depth, current_part);
                    current_part.push(c);
                }
                ')' if !in_quotes => {
                    paren_depth -= 1;
                    println!("[DEBUG parse_or_condition] 退出括号层({}): {}", paren_depth, current_part);
                    current_part.push(c);
                }
                // 处理OR关键字（不区分大小写）
                'O' | 'o' if !in_quotes && paren_depth == 0 => {
                    println!("[DEBUG parse_or_condition] 可能遇到OR关键字");
                    if let Some('R') | Some('r') = chars.peek() {
                        let next = chars.next().unwrap();
                        println!("[DEBUG parse_or_condition] 确认OR关键字: {}{}", c, next);
                        if chars.peek().map_or(true, |c| c.is_whitespace()) || chars.peek().is_none() {
                            // 确认是OR关键字
                            println!("[DEBUG parse_or_condition] 完成OR分割，当前部分: '{}'", current_part);
                            parts.push(current_part.trim().to_string());
                            current_part.clear();
                            continue;
                        }
                        current_part.push(c);
                        current_part.push(next);
                    } else {
                        current_part.push(c);
                    }
                }
                _ => {
                    current_part.push(c);
                }
            }
            //println!("[DEBUG parse_or_condition] 当前部分构建: '{}'", current_part);
        }
        
        if !current_part.is_empty() {
            //println!("[DEBUG parse_or_condition] 添加最后部分: '{}'", current_part);
            parts.push(current_part.trim().to_string());
        }

        //println!("[DEBUG parse_or_condition] 分割结果: {:?}", parts);

        if parts.len() < 2 {
            //println!("[DEBUG parse_or_condition] 错误: 无效的OR条件，分割部分不足2个");
            return Err(format!("Invalid OR condition in: '{}'", orig_cond));
        }

        // 3. 解析各个子条件
        let mut conditions = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            //println!("[DEBUG parse_or_condition] 解析子条件 {}: '{}'", i+1, part);
            let cond = Self::parse_condition(part, table).map_err(|e| {
                //println!("[DEBUG parse_or_condition] 子条件解析错误: {}", e);
                e
            })?;
            conditions.push(cond);
        }

        // 4. 组合条件 (使用any表示OR逻辑)
        Ok(Box::new(move |row| {
            conditions.iter().any(|cond| cond(row))
        }))
    }


    fn parse_single_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        // 原有 parse_condition 的实现内容
        let re = regex::Regex::new(r#"(?:("[^"]*")|('[^']*')|(\S+))"#).unwrap();
        let parts: Vec<&str> = re.find_iter(cond)
            .map(|m| m.as_str())
            .collect();

        if parts.len() != 3 && !(parts.len() == 4 && parts[1] == "IS" && (parts[3] == "NULL" || parts[3] == "NOT NULL")) {
            return Err(format!("Invalid WHERE format. Expected 'column op value', got: {:?}", parts));
        }

        let (col, op, raw_val) = (
            parts[0],
            parts[1],
            if parts.len() == 4 {
                parts[2..].join(" ")
            } else {
                parts[2].to_string()
            }
        );

        let val = raw_val.trim_matches(|c| c == '"' || c == '\'').to_string();
        let col_idx = table.columns.iter()
            .position(|c| c.name == col)
            .ok_or(format!("Column '{}' not found in table", col))?;

        Ok(match op {
            ">" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"').parse::<i32>().unwrap_or(0);
                let cond_val = val.parse::<i32>().unwrap_or(0);
                row_val > cond_val
            }),
            "<" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"').parse::<i32>().unwrap_or(0);
                let cond_val = val.parse::<i32>().unwrap_or(0);
                row_val < cond_val
            }),
            "=" => Box::new(move |row| {
                let row_val = row[col_idx].trim_matches('"');
                row_val == val
            }),
            "IS" if val == "NULL" => Box::new(move |row| {
                row[col_idx].trim_matches('"').is_empty()
            }),
            "IS" if val == "NOT NULL" => Box::new(move |row| {
                !row[col_idx].trim_matches('"').is_empty()
            }),
            _ => return Err(format!("Unsupported operator: {}", op)),
        })
    }
    
    fn parse_and_condition(
        cond: &str,
        table: &Table,
    ) -> Result<Box<dyn Fn(&[String]) -> bool>, String> {
        // 分割条件，处理可能的嵌套情况

        let cond = cond.trim();
        
        // 1. 先处理最外层的括号
        if cond.starts_with('(') && cond.ends_with(')') {
            return Self::parse_and_condition(&cond[1..cond.len()-1], table);
        }
        // 分割条件，处理可能的嵌套情况
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut in_quotes = false;
        let mut paren_depth = 0;
        let mut chars = cond.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '"' | '\'' => {
                    in_quotes = !in_quotes;
                    current_part.push(c);
                }
                '(' if !in_quotes => {
                    paren_depth += 1;
                    current_part.push(c);
                }
                ')' if !in_quotes => {
                    paren_depth -= 1;
                    current_part.push(c);
                }
                _ if c.to_ascii_uppercase() == 'A' 
                    && !in_quotes 
                    && paren_depth == 0 
                    && current_part.ends_with(' ') => {
                    
                    // 检查是否是完整的AND关键字
                    let mut and_chars = vec!['A'];
                    for _ in 0..2 {
                        if let Some(&next_c) = chars.peek() {
                            and_chars.push(next_c.to_ascii_uppercase());
                            chars.next();
                        }
                    }

                    if and_chars == ['A', 'N', 'D'] && chars.peek().map_or(true, |c| c.is_whitespace()) {
                        // 确认是AND关键字
                        parts.push(current_part.trim().to_string());
                        current_part.clear();
                    } else {
                        // 不是完整的AND，把字符加回去
                        current_part.push(c);
                        current_part.extend(&and_chars[1..]);
                    }
                }
                _ => current_part.push(c),
            }
        }
        
        // 添加最后一个部分
        if !current_part.is_empty() {
            parts.push(current_part.trim().to_string());
        }

        if parts.len() < 2 {
            return Err(format!("Invalid AND condition: '{}'", cond));
        }

        // 解析各个子条件
        let mut conditions = Vec::new();
        for part in parts {
            let cond = if part.to_uppercase().contains(" OR ") {
                Self::parse_or_condition(&part, table)?
            } else if part.to_uppercase().contains(" AND ") {
                Self::parse_and_condition(&part, table)?
            } else {
                Self::parse_single_condition(&part, table)?
            };
            conditions.push(cond);
        }

        // 组合条件 (使用all表示AND逻辑)
        Ok(Box::new(move |row| {
            conditions.iter().all(|cond| cond(row))
        }))
    }


}
