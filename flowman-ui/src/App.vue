<template>
  <v-app dark>
    <main-navigation-drawer/>

    <v-main>
      <router-view></router-view>
    </v-main>

    <template>
      <v-footer class="pa-3" app>
        <v-spacer></v-spacer>
        <div>&copy; {{ new Date().getFullYear() }}</div>
      </v-footer>
    </template>
  </v-app>
</template>

<script>
import MainNavigationDrawer from './components/MainNavigationDrawer'

export default {
  name: 'App',
  components: {
      MainNavigationDrawer
  }
};
</script>
