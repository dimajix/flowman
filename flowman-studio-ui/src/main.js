import Vue from 'vue'
import VueSSE from 'vue-sse'
import AsyncComputed from 'vue-async-computed'
import '@/plugins/axios'
import vuetify from '@/plugins/vuetify'
import Api from '@/services/api'
import Workbench from '@/App.vue'

Vue.config.productionTip = false

Vue.use(VueSSE)
Vue.use(Api)
Vue.use(AsyncComputed)

new Vue({
  vuetify,
  render: h => h(Workbench),
  data: {}
}).$mount('#app')
