module.exports = {
  outputDir: 'target/classes/META-INF/resources/webjars/flowman-studio-ui',
  transpileDependencies: [
    'vuetify'
  ],
  pages: {
    'index': {
      'entry': './src/main.js',
      'template': 'public/index.html',
      'title': 'Flowman Studio',
      'chunks': ['chunk-vendors', 'chunk-common', 'index']
    }
  },
  devServer: {
    port: 8088,
    proxy: {
      '^/api': {
        target: 'http://localhost:8080',
        ws: true,
        changeOrigin: true
      }
    }
  }
}
