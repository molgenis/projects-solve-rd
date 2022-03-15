module.exports = {
  root: true,

  env: {
    node: true
  },

  rules: {
    'no-console': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    'indent': ['error', 2],
    'no-trailing-spaces': [2, { 'skipBlankLines': true }],
    'eol-last': ['error', 'always']
  },

  extends: [
    'plugin:vue/essential',
    '@vue/standard'
  ]
}
