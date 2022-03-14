const webpack = require('webpack')
const BannerPlugin = require('webpack').BannerPlugin
const ZipPlugin = require('zip-webpack-plugin')
const packageJson = require('./package.json')
const GenerateJsonWebpackPlugin = require('generate-json-webpack-plugin')
const pkgVersion = packageJson.version
const pkgName = packageJson.name

const now = new Date()
const buildDate = now.toUTCString()
const bannerText = `
package-name: ${pkgName}
package-version: ${pkgVersion}
build-date: ${buildDate}`

const previewText = `
package-name: ${pkgName}
build-date: ${buildDate}
PR: ${process.env.CHANGE_ID}
BUILD: ${process.env.BUILD_NUMBER}`

const htmlTemplate = () => {
  if (process.env.NODE_ENV === 'production') return 'apptemplate/app-template.html'
  if (process.env.NODE_ENV === 'development') return 'public/index.html'
  if (process.env.NODE_ENV === 'test') return 'public/preview.html'
}

const PROXY_TARGET = 'https://solve-rd-acc.gcc.rug.nl'
/*
let apiDevServerProxyConf = {
  target: PROXY_TARGET,
  keepOrigin: true
}

if (process.env.DATA_EXPLORER_DEV_PW) {
  apiDevServerProxyConf.auth = 'admin:' + process.env.DATA_EXPLORER_DEV_PW
}
*/
module.exports = {
  transpileDependencies: ['@molgenis-ui/components-library'],
  runtimeCompiler: true,
  outputDir: 'dist',
  publicPath: process.env.NODE_ENV === 'production'
    ? '/plugin/app/' + packageJson.name
    : '/',
  chainWebpack: config => {
    config.resolve.symlinks(false)
    config
      .plugin('html')
      .tap(args => {
        args[0].template = htmlTemplate()
        args[0].version = process.env.NODE_ENV !== 'production' ? previewText : ''
        return args
      })
  },
  configureWebpack: config => {
    config.plugins.push(
      new BannerPlugin({
        banner: bannerText
      }),
      new webpack.ProvidePlugin({
        $: 'jquery',
        jQuery: 'jquery',
        'window.jQuery': 'jquery',
        Popper: ['popper.js', 'default']
      }),
      new GenerateJsonWebpackPlugin('config.json', {
        name: packageJson.name,
        label: packageJson.name,
        description: packageJson.description,
        version: packageJson.version,
        apiDependency: 'v2',
        includeMenuAndFooter: true,
        runtimeOptions: {}
      }),
      new ZipPlugin({
        filename: `${packageJson.name}.v${packageJson.version}`
      })
    )
  },
  devServer: {
    // In CI mode, Safari cannot contact "localhost", so as a workaround, run the dev server using the jenkins agent pod dns instead.
    host: process.env.JENKINS_AGENT_NAME || 'localhost',
    // Used to proxy a external API server to have someone to talk to during development
    proxy: process.env.NODE_ENV !== 'development' ? undefined : {
      '^/login': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/api': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/js': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/img': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/logo': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/fonts': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/theme': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/app-ui-context': {
        target: PROXY_TARGET,
        changeOrigin: true
      },
      '/logout': {
        target: PROXY_TARGET,
        changeOrigin: true
      }
    }
  }
}
