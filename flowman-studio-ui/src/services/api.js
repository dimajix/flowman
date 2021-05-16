import axios from 'axios';


export default {
  install(Vue) {
    const api = {
      state: Vue.observable({
          kernel: null,
          session: null
      }),

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

      setCurrentSession(kernel, session) {
        this.state.kernel = kernel
        this.state.session = session
      },

      getKernelLog() {
        return Vue.$sse.create({
          url:'/api/kernel/' + this.state.kernel + '/log',
          format: 'json'
        })
      }
    };

    Vue.prototype.$api = api
  }
};
