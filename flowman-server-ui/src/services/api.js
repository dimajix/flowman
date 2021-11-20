import axios from 'axios';

export default {
  install(Vue) {

    const api = {
      getNamespace() {
        return axios.get('/api/namespace')
          .then(response => response.data)
      },

      getJobDetails(job) {
        return axios.get('/api/history/job/' + job)
          .then(response => response.data)
      },

      getJobCounts(grouping) {
        return axios.get('/api/history/job-counts?grouping=' + grouping)
            .then(response => response.data)
      },

      getAllJobsHistory() {
        return axios.get('/api/history/jobs')
          .then(response => response.data)
      },

      getAllProjectJobsHistory(project) {
        return axios.get('/api/history/jobs?project=' + project)
          .then(response => response.data)
      },

      getProjectJobHistory(project, job) {
        return axios.get('/api/history/jobs?project=' + project + '&job=' + job)
          .then(response => response.data)
      },

      getAllTargetsHistory() {
        return axios.get('/api/history/targets')
          .then(response => response.data)
      },

      getAllProjectTargetsHistory(project) {
        return axios.get('/api/history/targets?project=' + project)
          .then(response => response.data)
      },

      getProjectTargetsHistory(project, target) {
        return axios.get('/api/history/targets?project=' + project + '&target=' + target)
          .then(response => response.data)
      }
    };

    Vue.prototype.$api = api
  }
};
