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
        values: Vec<String>,
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
    
    let primary_keys: Vec<&str> = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::Unique {
                is_primary: true,
                columns,
                ..
            } => Some(columns.iter().map(|c| c.value.as_str()).collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .collect();

    let mut parsed_columns = Vec::new();
    for col in columns {
        let col_name = col.name.value;
        
        // 关键修复：正确处理 CharacterLength 类型
        let data_type = match &col.data_type {
            DataType::Int(_) => DbDataType::Int(10),
            DataType::Varchar(Some(len_info)) => {
                // 从 CharacterLength 结构体中提取 length 字段
                DbDataType::Varchar(len_info.length as u32)
            }
            DataType::Varchar(None) => DbDataType::Varchar(255),
            _ => return Err(format!("Unsupported data type: {}", col.data_type)),
        };
        
        let is_primary = primary_keys.contains(&col_name.as_str());
        let not_null = col
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::NotNull));
        
        parsed_columns.push((col_name, data_type, is_primary, not_null));
    }
    
    Ok(SqlAst::CreateTable {
        table_name,
        columns: parsed_columns,
    })
}

fn parse_insert(table_name: ObjectName, source: Box<Query>) -> Result<SqlAst, String> {
    let table = table_name.to_string();
    
    let values = match source.body.as_ref() {
        SetExpr::Values(values) => {
            values.rows
                .iter()
                .flat_map(|row| {
                    row.iter()
                        .map(|expr| expr.to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        }
        _ => return Err("Only VALUES clause is supported".into()),
    };
    
    Ok(SqlAst::Insert { table, values })
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
