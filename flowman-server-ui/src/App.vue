<template>
  <v-app>
    <main-navigation-drawer/>

    <v-main>
      <v-container class="fill-height" fluid>
        <v-row class="fill-height">
          <v-col>
            <router-view></router-view>
          </v-col>
        </v-row>
      </v-container>
    </v-main>
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
