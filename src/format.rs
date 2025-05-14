use crate::database::{Database, Table};

pub fn format_table(
    headers: Vec<String>,
    data: Vec<Vec<String>>,
) -> String {
    // 计算每列最大宽度
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    for row in &data {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    // 生成分隔线
    let separator: String = widths.iter()
        .map(|&w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");

    // 构建表格
    let mut result = Vec::new();

    // 表头
    let header_line: String = headers.iter().enumerate()
        .map(|(i, h)| format!(" {:1$} ", h, widths[i]))
        .collect::<Vec<_>>()
        .join("|");

    result.push(separator.clone());
    result.push(header_line);
    result.push(separator.clone());

    // 数据行
    for row in data {
        let row_line: String = row.iter().enumerate()
            .map(|(i, cell)| format!(" {:1$} ", cell, widths[i]))
            .collect::<Vec<_>>()
            .join("|");
        result.push(row_line);
    }

    result.push(separator);
    result.join("\n")
}

pub fn format_table_from_db(
    db: &Database,
    table_name: &str,
    columns: Vec<&str>,
    data: Vec<Vec<String>>,
) -> Result<String, String> {
    let table = db.tables
        .iter()
        .find(|t| t.name == table_name)
        .ok_or(format!("Table '{}' not found", table_name))?;

    // 获取列名作为表头
    let headers = if columns == ["*"] {
        // 选择所有列
        table.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        // 选择指定列
        columns.iter().map(|&col_name| {
            table.columns.iter()
                .find(|c| c.name == col_name)
                .map(|c| c.name.clone())
                .ok_or(format!("Column '{}' not found", col_name))
        }).collect::<Result<Vec<_>, _>>()?
    };

    // 验证列数匹配
    if !data.is_empty() && headers.len() != data[0].len() {
        return Err("Column count mismatch between headers and data".into());
    }

    Ok(format_table(headers, data))
}