import axios from 'axios';

export default {
  install(Vue) {

    const api = {
      listProjects() {
        return axios.get('/api/project')
          .then(response => response.data)
      },

      getProject(name) {
        return axios.get('/api/project/' + name)
          .then(response => response.data)
      },

      getAllJobsHistory() {
        return axios.get('/api/job-history')
          .then(response => response.data)
      },

      getAllProjectJobsHistory(project) {
        return axios.get('/api/job-history/' + project)
          .then(response => response.data)
      },

      getProjectJobHistory(project, job) {
        return axios.get('/api/job-history/' + project + '/' + job)
          .then(response => response.data)
      },

      getAllTargetsHistory() {
        return axios.get('/api/target-history')
          .then(response => response.data)
      },

      getAllProjectTargetsHistory(project) {
        return axios.get('/api/target-history/' + project)
          .then(response => response.data)
      },

      getProjectTargetsHistory(project, target) {
        return axios.get('/api/target-history/' + project + '/' + target)
          .then(response => response.data)
      }
    };

    Vue.prototype.$api = api
  }
};
