import Vue from 'vue'
import './plugins/axios'
import vuetify from './plugins/vuetify'
import Api from './services/api'
import App from './App.vue'
import router from './router'

Vue.config.productionTip = false

Vue.use(Api)

new Vue({
  vuetify,
  router,
  render: h => h(App)
}).$mount('#app')
