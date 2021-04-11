import Vue from 'vue'
import './plugins/axios'
import vuetify from './plugins/vuetify'
import Api from './services/api'
import Studio from './Studio.vue'
import router from './router'

Vue.config.productionTip = false

Vue.use(Api)

new Vue({
  vuetify,
  router,
  render: h => h(Studio)
}).$mount('#app')
