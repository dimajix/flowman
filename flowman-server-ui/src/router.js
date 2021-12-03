import Vue from 'vue'
import Router from 'vue-router'
import Home from './views/Home.vue'
import System from './views/System.vue'
import JobHistory from './views/JobHistory.vue'
import TargetHistory from './views/TargetHistory.vue'
import Metrics from './views/Metrics.vue'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      redirect: '/home'
    },
    {
      path: '/home',
      name: 'home',
      component: Home
    },
    {
      path: '/system',
      name: 'system',
      component: System
    },
    {
      path: '/job-history',
      name: 'job-history',
      component: JobHistory
    },
    {
      path: '/target-history',
      name: 'target-history',
      component: TargetHistory
    },
    {
      path: '/metrics',
      name: 'metrics',
      component: Metrics
    }
  ]
})
