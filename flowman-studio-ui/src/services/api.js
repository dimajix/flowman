import axios from 'axios';

export default {
  install(Vue) {

    const api = {
      getNamespace() {
        return axios.get('/api/namespace')
          .then(response => response.data)
      }
    };

    Vue.prototype.$api = api
  }
};
