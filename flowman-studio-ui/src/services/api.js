import axios from 'axios';


export default {
  install(Vue) {
    const api = {
      state: Vue.observable({
          session: null
      }),

      getNamespace() {
        return axios.get('/api/namespace')
          .then(response => response.data)
      },

      listSessions() {
        return axios.get('/api/session')
          .then(response => response.data)
      },
      resetSession(session) {
        return axios.post('/api/session/' + session + "/reset")
      },
      closeSession(session) {
        return axios.delete('/api/session/' + session)
      },

      setCurrentSession(session) {
        this.state.session = session
      },

      getKernelLog() {
        return Vue.$sse.create({
          url:'/api/log',
          format: 'json'
        })
      },

      listProjects() {
        return axios.get('/api/project')
          .then(response => response.data)
      },
      openProject(project) {
        return axios.post('/api/session', {projectName: project})
          .then(response => response.data)
      },

      getCurrentSession() {
        return axios.get('/api/session/' + this.state.session)
          .then(response => response.data)
      },
      listJobs() {
        return axios.get('/api/session/' + this.state.session + "/job")
          .then(response => response.data)
      },
      getJob(job) {
        return axios.get('/api/session/' + this.state.session + "/job/" + job)
          .then(response => response.data)
      },
      listTargets() {
        return axios.get('/api/session/' + this.state.session + "/target")
          .then(response => response.data)
      },
      getTarget(target) {
        return axios.get('/api/session/' + this.state.session + "/target/" + target)
          .then(response => response.data)
      },
      listTests() {
        return axios.get('/api/session/' + this.state.session + "/test")
          .then(response => response.data)
      },
      getTest(test) {
        return axios.get('/api/session/' + this.state.session + "/test/" + test)
          .then(response => response.data)
      },
      listMappings() {
        return axios.get('/api/session/' + this.state.session + "/mapping")
          .then(response => response.data)
      },
      getMapping(mapping) {
        return axios.get('/api/session/' + this.state.session + "/mapping/" + mapping)
          .then(response => response.data)
      },
      listRelations() {
        return axios.get('/api/session/' + this.state.session + "/relation")
          .then(response => response.data)
      },
      getRelation(relation) {
        return axios.get('/api/session/' + this.state.session + "/relation/" + relation)
          .then(response => response.data)
      },
    };

    Vue.prototype.$api = api
  }
};
