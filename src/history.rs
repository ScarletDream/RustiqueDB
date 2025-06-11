use std::collections::VecDeque;

#[derive(Debug)]
pub struct CommandHistory {
    commands: VecDeque<String>,  // 使用双端队列便于两端操作
    max_size: usize,
    current_index: usize,       // 当前浏览索引
}

impl CommandHistory {
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn new(max_size: usize) -> Self {
        Self {
            commands: VecDeque::with_capacity(max_size),
            max_size,
            current_index: 0,
        }
    }

    pub fn is_history_command(cmd: &str) -> bool {
        let trimmed = cmd.trim();
        trimmed == "!!" || trimmed.starts_with('!') && trimmed[1..].trim().parse::<usize>().is_ok()
    }

    fn should_skip_command(cmd: &str) -> bool {
        let upper = cmd.to_uppercase();
        upper == "HISTORY" || upper == "HISTORY;" || Self::is_history_command(cmd)
    }

    pub fn add(&mut self, command: impl Into<String>) {
        let cmd = command.into();
        let cleaned = cmd.trim();

        if cleaned.is_empty() || Self::should_skip_command(cleaned) {
            return;
        }

        // 处理多行命令
        let to_store = if cleaned.ends_with(';') || cleaned.to_lowercase() == "exit" {
            cleaned.to_string()
        } else {
            format!("{};", cleaned)
        };

        // 去重检查（比较去除分号的版本）
        let check_dup = cleaned.trim_end_matches(';').trim();
        if self.commands.back()
            .map(|s| s.trim_end_matches(';').trim() == check_dup)
            .unwrap_or(false)
        {
            return;
        }

        if self.commands.len() >= self.max_size {
            self.commands.pop_front();
        }
        self.commands.push_back(to_store);
        self.reset_index();
    }

    pub fn get_previous(&mut self) -> Option<&str> {
        if self.current_index > 0 {
            self.current_index -= 1;
            self.commands.get(self.current_index).map(|s| s.as_str())
        } else {
            None
        }
    }

    pub fn get_next(&mut self) -> Option<&str> {
        if self.current_index < self.commands.len() {
            self.current_index += 1;
            self.commands.get(self.current_index.saturating_sub(1)).map(|s| s.as_str())
        } else {
            None
        }
    }

    pub fn reset_index(&mut self) {
        self.current_index = self.commands.len();
    }

    pub fn get_full_command(&self, index: usize) -> Option<String> {
        self.commands.get(index).map(|cmd| {
            let cleaned = cmd.trim();
            // 确保返回完整可执行的命令
            if !cleaned.ends_with(';') && !cleaned.eq_ignore_ascii_case("exit") {
                format!("{};", cleaned)
            } else {
                cleaned.to_string()
            }
        })
    }

    pub fn clear(&mut self) {
        self.commands.clear();
        self.current_index = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.commands.iter()
    }

    pub fn enumerate(&self) -> impl Iterator<Item = (usize, &String)> {
        self.commands.iter().enumerate()
    }
}
