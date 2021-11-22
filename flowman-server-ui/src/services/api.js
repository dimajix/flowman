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

      getJobCounts(grouping, projects=[], jobs=[], phase=[], status=[]) {
        return axios.get('/api/history/job-counts?grouping=' + grouping + "&project=" + projects.join(',') + "&job=" + jobs.join(',') + "&phase=" + phase.join(',') + "&status=" + status.join(','))
            .then(response => response.data)
      },

      getJobsHistory(projects=[], jobs=[], phase=[], status=[]) {
        return axios.get('/api/history/jobs?project=' + projects.join(',') + "&job=" + jobs.join(',') + "&phase=" + phase.join(',') + "&status=" + status.join(','))
          .then(response => response.data)
      },

      getTargetsHistory(projects=[], jobs=[], targets=[], phase=[], status=[]) {
        return axios.get('/api/history/targets?&project=' + projects.join(',') + "&job=" + jobs.join(',') + "&target=" + targets.join(',') + "&phase=" + phase.join(',') + "&status=" + status.join(','))
          .then(response => response.data)
      },

      getTargetCounts(grouping, projects=[], jobs=[], targets=[], phase=[], status=[]) {
        return axios.get('/api/history/target-counts?grouping=' + grouping + "&project=" + projects.join(',') + "&job=" + jobs.join(',') + "&target=" + targets.join(',') + "&phase=" + phase.join(',') + "&status=" + status.join(','))
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
