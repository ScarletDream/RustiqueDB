module.exports = {
    extends: ["@commitlint/config-conventional"],
    rules: {
      // 提交信息的格式必须为 [type] TEXT
      "header-max-length": [2, "always", 72], // 提交信息标题最大长度为 72 个字符
      "type-enum": [
        2,
        "always",
        [
          "feat", // 新功能
          "fix", // 修复 bug
          "docs", // 文档变更
          "style", // 代码格式（不影响代码运行的变动）
          "refactor", // 重构（既不是新增功能，也不是修复 bug 的代码变动）
          "test", // 增加测试
          "chore", // 构建过程或辅助工具的变动
        ],
      ],
      "type-case": [2, "always", "lower-case"], // type 必须为小写
      "type-empty": [2, "never"], // type 不能为空
      "subject-min-length": [2, "always", 10], // TEXT 部分最小长度为 10 个字符
      "subject-case": [2, "never", ["sentence-case", "start-case", "pascal-case", "upper-case"]], // TEXT 部分不强制特定大小写格式
    },
};
  