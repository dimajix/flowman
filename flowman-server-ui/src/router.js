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
      path: '/job-history/:project',
      name: 'job-history',
      component: JobHistory,
      props: route => ({ project: route.params.project })
    },
    {
      path: '/target-history/:project',
      name: 'target-history',
      component: TargetHistory,
      props: route => ({ project: route.params.project })
    },
    {
      path: '/metrics/:project',
      name: 'metrics',
      component: Metrics,
      props: route => ({ project: route.params.project })
    }
  ]
})
