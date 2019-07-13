import Vue from 'vue'
import Router from 'vue-router'
import Home from './views/Home.vue'
import Namespace from './views/Namespace.vue'
import System from './views/System.vue'
import Project from './views/Project.vue'
import JobHistory from './views/JobHistory.vue'
import TargetHistory from './views/TargetHistory.vue'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'home',
      component: Home
    },
    {
      path: '/namespace',
      name: 'namespace',
      component: Namespace
    },
    {
      path: '/system',
      name: 'system',
      component: System
    },
    {
      path: '/project/:name',
      name: "project",
      component: Project,
      props: route => ({
        projectName: route.params.name
      })
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
    }
  ]
})
