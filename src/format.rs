use crate::database::{Database, Table};

pub fn format_table(
    headers: Vec<String>,
    data: Vec<Vec<String>>,
) -> String {
    // 计算每列最大内容宽度（纯内容，不考虑空格）
    let mut content_widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    for row in &data {
        for (i, cell) in row.iter().enumerate() {
            content_widths[i] = content_widths[i].max(cell.trim().len());
        }
    }

    // 确保每列最小内容宽度为3
    for width in &mut content_widths {
        *width = (*width).max(3);
    }

    // 构建表格各部分
    let mut result = Vec::new();

    // 表头行和数据行中的单元格格式（内容左右各1空格）
    let format_cell = |content: &str, width: usize| {
        format!(" {:<width$} ", content, width = width)
    };

    // 表头行
    let header_line: String = headers.iter().enumerate()
        .map(|(i, h)| format_cell(h, content_widths[i]))
        .collect::<Vec<_>>()
        .join("|");
    let header_line = format!("|{}|", header_line);

    // 分隔线（完全匹配数据行的格式）
    let separator_line: String = content_widths.iter()
        .map(|&w| "-".repeat(w))  // 分隔线长度等于内容宽度
        .collect::<Vec<_>>()
        .join(" | ");  // 所有分隔符两侧都加空格
    let separator_line = format!("| {} |", separator_line); // 首尾也加空格

    // 数据行
    let data_lines: Vec<String> = data.iter()
        .map(|row| {
            row.iter().enumerate()
                .map(|(i, cell)| format_cell(cell.trim(), content_widths[i]))
                .collect::<Vec<_>>()
                .join("|")
        })
        .map(|line| format!("|{}|", line))
        .collect();

    result.push(header_line);
    result.push(separator_line);
    result.extend(data_lines);
    
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
        table.columns.iter().map(|c| c.name.clone()).collect()
    } else {
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
