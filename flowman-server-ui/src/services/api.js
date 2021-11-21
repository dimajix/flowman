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

      getJobsHistory() {
        return axios.get('/api/history/jobs')
          .then(response => response.data)
      },

      getTargetsHistory() {
        return axios.get('/api/history/targets')
          .then(response => response.data)
      },

      getTargetCounts(grouping) {
        return axios.get('/api/history/target-counts?grouping=' + grouping)
          .then(response => response.data)
      },

      getJobTargets(job) {
        return axios.get('/api/history/targets?jobId=' + job)
          .then(response => response.data)
      },
    };

    Vue.prototype.$api = api
  }
};
