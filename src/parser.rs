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
        table_name: String,
        if_exists: bool,  // 是否包含 IF EXISTS 子句
    },
}

pub fn parse_sql(input: &str) -> Result<SqlAst, String> {
    let dialect = GenericDialect {};
    //println!("Using dialect: {}", std::any::type_name::<PostgreSqlDialect>());
    let mut parser = Parser::new(&dialect);
    let ast = parser
        .try_with_sql(input)
        .map_err(|e| e.to_string())?
        .parse_statement()
        .map_err(|e| e.to_string())?;

    //用于查看语法树的结构
    //println!("{:#?}", ast);
    match ast {
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
        Statement::Drop {object_type,if_exists,names,..} => {
            parse_drop(object_type, if_exists, names)   
        }  
        _ => Err("Unsupported SQL command".to_string()),
    }
}

fn parse_select(query: &Query) -> Result<SqlAst, String> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
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

            Ok(SqlAst::Select {
                table,
                columns,
                where_clause,
            })
        }
        _ => Err("Unsupported query type".into()),
    }
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
    
    let where_clause = selection.map(|expr| expr.to_string());
    
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

fn parse_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
) -> Result<SqlAst, String> {
    // 目前只支持 DROP TABLE
    if object_type != ObjectType::Table {
        return Err("Only DROP TABLE is supported".into());
    }

    if names.len() != 1 {
        return Err("DROP TABLE only supports single table".into());
    }

    let table_name = names[0].to_string(); // 简化处理，实际可能需要处理带schema的情况

    Ok(SqlAst::Drop { 
        table_name,
        if_exists,
    })
}
