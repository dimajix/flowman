export default {
  install(Vue, options) {

    const api = {
      listProjects() {
        return axios.get('/api/project')
          .then(response => response.data)
      },

      getProject(name) {
        return axios.get('/api/project/' + name)
          .then(response => response.data)
      }
    };

    Vue.prototype.$api = api
  }
};
