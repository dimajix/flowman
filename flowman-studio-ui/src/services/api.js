import axios from 'axios';

export default {
  install(Vue) {

    const api = {
      getNamespace() {
        return axios.get('/api/namespace')
          .then(response => response.data)
      },

      launchKernel() {
        return axios.post('/api/kernel')
          .then(response => response.data)
      },
      shutdownKernel(kernel) {
        return axios.delete('/api/kernel/' + kernel)
          .then(response => response.data)
      },
      listKernels() {
        return axios.get('/api/kernel')
          .then(response => response.data)
      },

      listSessions(kernel) {
        return axios.get('/api/kernel/' + kernel + "/session")
          .then(response => response.data)
      },

      getCurrentSession() {

      },
      getCurrentKernel() {

      },
      setCurrentSession(kernel, session) {
        kernel == session
      }
    };

    Vue.prototype.$api = api
  }
};
