import Vue from 'vue';
import Vuetify from 'vuetify/lib';

Vue.use(Vuetify);


const vuetify = new Vuetify({
  theme: {
    dark: false,
    themes: {
      light: {
        'brand-blue': '#071f4e',
        'brand-green': '#0C6545',
        'brand-light-blue': '#008AFC'
      },
    },
  },
});

export default vuetify;
