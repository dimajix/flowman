<template>
  <v-app dark>
    <v-toolbar app >
      <v-toolbar-title class="headline text-uppercase">
        <span>Flowman</span>
      </v-toolbar-title>
      <v-spacer></v-spacer>
    </v-toolbar>

    <main-navigation-drawer/>

    <v-content>
      <router-view></router-view>
    </v-content>

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
}
</script>
