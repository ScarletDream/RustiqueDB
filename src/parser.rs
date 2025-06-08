use crate::database::DataType as DbDataType;
use sqlparser::{
    ast::*,
    dialect::GenericDialect,
    parser::Parser,
};

#[derive(Debug)]
pub enum SqlAst {
    Select {
        table: String,
        columns: Vec<String>,
        where_clause: Option<String>,
        order_by: Vec<(String, bool)>,
    },
    Calculate {
        expression: String,  // 原始表达式
        result: f64          // 计算结果
    },
    CreateTable {
        table_name: String,
        columns: Vec<(String, DbDataType, bool, bool)>,
    },
    Insert {
        table: String,
        values: Vec<Vec<String>>,  // 修改为支持多行
    },
    Update {
        table: String,
        set: Vec<(String, String)>,
        where_clause: Option<String>,
    },
    Delete {
        table: String,
        where_clause: Option<String>,
    },
    Drop {
        tables: Vec<String>,
        if_exists: bool,  // 保留此字段
    },
}

const OPERATOR_PRECEDENCE: &[(char, u8)] = &[
    ('*', 3),
    ('/', 3),
    ('+', 2),
    ('-', 2),
];

fn get_precedence(op: char) -> u8 {
    OPERATOR_PRECEDENCE.iter()
        .find(|(c, _)| *c == op)
        .map(|(_, p)| *p)
        .unwrap_or(0)
}

// Token枚举
#[derive(Debug)]
enum Token {
    Number(f64),
    Operator(char),
    LeftParen,
    RightParen,
}

// 分词函数
fn tokenize(expr: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut num_buffer = String::new();

    for c in expr.chars() {
        match c {
            '0'..='9' | '.' => num_buffer.push(c),
            '+' | '-' | '*' | '/' | '(' | ')' => {
                if !num_buffer.is_empty() {
                    tokens.push(Token::Number(num_buffer.parse().map_err(|_| "Invalid number")?));
                    num_buffer.clear();
                }
                match c {
                    '(' => tokens.push(Token::LeftParen),
                    ')' => tokens.push(Token::RightParen),
                    op => tokens.push(Token::Operator(op)),
                }
            },
            ' ' => continue,  // 忽略空格
            _ => return Err(format!("Unknown character: {}", c)),
        }
    }

    // 处理最后一个数字
    if !num_buffer.is_empty() {
        tokens.push(Token::Number(num_buffer.parse().map_err(|_| "Invalid number")?));
    }

    Ok(tokens)
}

// 运算符应用函数
fn apply_operator(op: char, left: f64, right: f64) -> Result<f64, String> {
    match op {
        '+' => Ok(left + right),
        '-' => Ok(left - right),
        '*' => Ok(left * right),
        '/' => {
            if right == 0.0 {
                Err("Division by zero".into())
            } else {
                Ok(left / right)
            }
        },
        _ => Err(format!("Unknown operator: {}", op))
    }
}

pub fn parse_sql(input: &str) -> Result<SqlAst, String> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect);

    // 首先尝试解析为常规SQL语句
    match parser.try_with_sql(input)
        .map_err(|e| e.to_string())
        .and_then(|mut p| p.parse_statement().map_err(|e| e.to_string()))
    {
        Ok(ast) => match ast {
            Statement::Query(query) => parse_select(&query),
            Statement::CreateTable { name, columns, constraints, .. } => {
                parse_create_table(name, columns, constraints)
            }
            Statement::Insert { table_name, source, .. } => parse_insert(table_name, source),
            Statement::Update { table, assignments, selection, .. } => {
                parse_update(table, assignments, selection)
            }
            Statement::Delete { from, selection, .. } => {
                if from.len() != 1 {
                    return Err("DELETE statement only supports single table".into());
                }
                let table_with_joins = from.into_iter().next().unwrap();
                parse_delete(table_with_joins, selection)
            }
            Statement::Drop { object_type, if_exists, names, ..}
            if object_type == ObjectType::Table => {
                parse_drop_table(names, if_exists)
            }
            _ => parse_calculation(input.trim()) // 如果不是支持的SQL语句，尝试解析为计算表达式
        },
        Err(_) => parse_calculation(input.trim()) // 如果解析失败，尝试解析为计算表达式
    }
}

fn parse_select(query: &Query) -> Result<SqlAst, String> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            // 检查是否为无表查询（纯计算）
            if select.from.is_empty() {
                if select.projection.len() == 1 {
                    if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                        return parse_calculation(&expr.to_string());
                    }
                }
                return Err("Calculation expressions must have exactly one column".into());
            }

            let table = select
                .from
                .first()
                .and_then(|t| match &t.relation {
                    TableFactor::Table { name, .. } => Some(name.to_string()),
                    _ => None,
                })
                .ok_or("Missing table name in FROM clause")?;

            let columns = select
                .projection
                .iter()
                .map(|p| match p {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Ok(ident.value.clone()),
                    SelectItem::Wildcard(_) => Ok("*".to_string()),
                    _ => Err("Unsupported column expression".to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;

            let where_clause = select
                .selection
                .as_ref()
                .map(|expr| expr.to_string());

            let mut order_by = Vec::new();
            for expr in &query.order_by {  // 直接迭代&Vec
                match &expr.expr {
                    Expr::Identifier(ident) => {
                        order_by.push((ident.value.clone(), !expr.asc.unwrap_or(true)));
                    },
                    _ => return Err("Only column names are supported in ORDER BY".into()),
                }
            }

            Ok(SqlAst::Select {
                table,
                columns,
                where_clause,
                order_by,
            })
        }
        _ => Err("Unsupported query type".into()),
    }
}

// 计算表达式解析函数
fn parse_calculation(input: &str) -> Result<SqlAst, String> {
    // 支持带SELECT前缀或纯表达式
    let expr = input.strip_prefix("SELECT ")
        .unwrap_or(input)
        .trim_end_matches(';')
        .trim();

    // 验证表达式有效性
    if expr.is_empty() {
        return Err("Empty expression".into());
    }

    // 检查括号匹配
    let mut paren_stack = 0;
    for c in expr.chars() {
        match c {
            '(' => paren_stack += 1,
            ')' => {
                if paren_stack == 0 {
                    return Err("Unmatched closing parenthesis".into());
                }
                paren_stack -= 1;
            },
            _ => {}
        }
    }
    if paren_stack != 0 {
        return Err("Unmatched opening parenthesis".into());
    }

    let result = eval_expression(expr)?;
    Ok(SqlAst::Calculate {
        expression: expr.to_string(),
        result
    })
}

// 简单表达式求值（支持+-*/）
fn eval_expression(expr: &str) -> Result<f64, String> {
    let tokens = tokenize(expr)?;
    let mut output = Vec::new();
    let mut operators = Vec::new();

    for token in tokens {
        match token {
            Token::Number(num) => output.push(num),
            Token::Operator(op) => {
                while let Some(top_op) = operators.last() {
                    if *top_op == '(' {
                        break;
                    }
                    if get_precedence(*top_op) >= get_precedence(op) {
                        let op = operators.pop().unwrap();
                        let (right, left) = (output.pop().ok_or("Missing operand")?,
                                           output.pop().ok_or("Missing operand")?);
                        output.push(apply_operator(op, left, right)?);
                    } else {
                        break;
                    }
                }
                operators.push(op);
            }
            Token::LeftParen => operators.push('('),
            Token::RightParen => {
                while let Some(op) = operators.pop() {
                    if op == '(' {
                        break;
                    }
                    let (right, left) = (output.pop().ok_or("Missing operand")?,
                                       output.pop().ok_or("Missing operand")?);
                    output.push(apply_operator(op, left, right)?);
                }
            }
        }
    }

    while let Some(op) = operators.pop() {
        let (right, left) = (output.pop().ok_or("Missing operand")?,
                           output.pop().ok_or("Missing operand")?);
        output.push(apply_operator(op, left, right)?);
    }

    output.pop().ok_or("Invalid expression".into())
}

fn parse_create_table(
    name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
) -> Result<SqlAst, String> {
    let table_name = name.to_string();
    //println!("[DEBUG] 开始解析创建表: {}", table_name);

    // 1. 收集所有主键列名（从列级约束和表级约束）
    let mut primary_keys = Vec::new();

    // 1.1 先处理列级主键约束
    for col in &columns {
        for option in &col.options {
            if let ColumnOption::Unique { is_primary: true } = option.option {
                //println!("[DEBUG] 发现列级主键约束: {}", col.name.value);
                primary_keys.push(col.name.value.clone());
            }
        }
    }

    // 1.2 再处理表级主键约束（如果有）
    for constraint in &constraints {
        if let TableConstraint::Unique {
            is_primary: true,
            columns,
            ..
        } = constraint {
            //println!("[DEBUG] 发现表级主键约束: {:?}", columns);
            primary_keys.extend(columns.iter().map(|c| c.value.clone()));
        }
    }

    //println!("[DEBUG] 最终主键列: {:?}", primary_keys);

    // 2. 处理列定义
    let mut parsed_columns = Vec::new();
    for col in columns {
        let col_name = col.name.value;
        
        // 检查是否是主键列
        let is_primary = primary_keys.contains(&col_name);
        
        // 主键自动设置为NOT NULL（即使没有显式指定）
        let mut not_null = is_primary;
        
        // 检查显式的NOT NULL约束
        for option in &col.options {
            match &option.option {
                ColumnOption::NotNull => {
                    not_null = true;
                    //println!("[DEBUG] 列 '{}' 显式设置了 NOT NULL", col_name);
                }
                _ => {}
            }
        }

        let data_type = match &col.data_type {
            DataType::Int(_) => DbDataType::Int(10),
            DataType::Varchar(Some(len_info)) => DbDataType::Varchar(len_info.length as u32),
            DataType::Varchar(None) => DbDataType::Varchar(255),
            _ => return Err(format!("Unsupported data type: {}", col.data_type)),
        };
        
        //println!(
          //  "[DEBUG] 列处理完成: name={}, type={:?}, primary={}, not_null={}",
          //  col_name, data_type, is_primary, not_null
        //);
        
        parsed_columns.push((col_name, data_type, is_primary, not_null));
    }
    
    Ok(SqlAst::CreateTable {
        table_name,
        columns: parsed_columns,
    })
}



fn parse_insert(table_name: ObjectName, source: Box<Query>) -> Result<SqlAst, String> {
    let table = table_name.to_string();
    
    match *source.body {
        SetExpr::Values(values) => {
            let parsed_values = values.rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| expr.to_string())
                        .collect()
                })
                .collect();
            
            Ok(SqlAst::Insert {
                table,
                values: parsed_values,
            })
        }
        _ => Err("Only VALUES clause is supported".into()),
    }
}


fn parse_update(
    table: TableWithJoins,
    assignments: Vec<Assignment>,
    selection: Option<Expr>,
) -> Result<SqlAst, String> {
    let table_name = match table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("Invalid table reference".into()),
    };
    
    let set = assignments
        .into_iter()
        .map(|assg| {
            if assg.id.len() != 1 {
                return Err(format!(
                    "Expected single column name, found {}",
                    assg.id.len()
                ));
            }
            let column_name = assg.id[0].value.clone();
            let value = assg.value.to_string();
            Ok((column_name, value))
        })
        .collect::<Result<Vec<(String, String)>, String>>()?;
    
    let where_clause = selection.map(|expr| {
        // 标准化条件表达式字符串
        expr.to_string()
            .replace('\'', "\"") // 正确写法：第一个参数是char，第二个是&str
            .replace("IS NULL", "IS \"\"")  // 处理NULL情况
            .replace("IS NOT NULL", "IS NOT \"\"")
    });
    
    Ok(SqlAst::Update {
        table: table_name,
        set,
        where_clause,
    })
}

fn parse_delete(table_with_joins: TableWithJoins, selection: Option<Expr>) -> Result<SqlAst, String> {
    let table_name = match table_with_joins.relation {
        TableFactor::Table { name, .. } => {
            match &name.0[..] {
                [ident] => ident.value.clone(),
                [schema, table] => format!("{}.{}", schema.value, table.value),
                _ => return Err("Invalid table name format".into()),
            }
        }
        _ => return Err("DELETE only supports simple table targets".into()),
    };

    Ok(SqlAst::Delete {
        table: table_name,
        where_clause: selection.map(|e| e.to_string()),
    })
}

fn parse_drop_table(names: Vec<ObjectName>, if_exists: bool) -> Result<SqlAst, String> {
    let tables = names
        .into_iter()
        .map(|name| name.to_string())
        .collect();
    
    Ok(SqlAst::Drop { tables, if_exists })
}
