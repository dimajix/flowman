<template>
  <v-app dark>
    <v-toolbar app >
      <v-toolbar-title class="headline text-uppercase">
        <span>Flowman</span>
      </v-toolbar-title>
      <v-spacer></v-spacer>
    </v-toolbar>

    <v-navigation-drawer
            app
            stateless
            clipped
            value=true
    >
      <v-list>
        <v-list-tile :to="{path: '/system'}">
          <v-list-tile-action>
            <v-icon>info</v-icon>
          </v-list-tile-action>
          <v-list-tile-title>
            System
          </v-list-tile-title>
        </v-list-tile>

        <v-list-tile :to="{path: '/namespace'}">
          <v-list-tile-action>
            <v-icon>home</v-icon>
          </v-list-tile-action>
          <v-list-tile-title>
            Namespace
          </v-list-tile-title>
        </v-list-tile>

        <v-list-group no-action>
          <template v-slot:activator>
            <v-list-tile>
              <v-list-tile-action>
                <v-icon>work</v-icon>
              </v-list-tile-action>
              <v-list-tile-title>Projects</v-list-tile-title>
            </v-list-tile>
          </template>

          <v-list-tile
                  v-for="project in projects"
                  :key="project.name"
          >
            <v-list-tile-content>
              <v-list-tile-title>{{ project.name }}</v-list-tile-title>
            </v-list-tile-content>
          </v-list-tile>
        </v-list-group>

        <v-list-group no-action>
          <template v-slot:activator>
            <v-list-tile>
              <v-list-tile-action>
                <v-icon>history</v-icon>
              </v-list-tile-action>
              <v-list-tile-title>
                History
              </v-list-tile-title>
            </v-list-tile>
          </template>

          <v-list-tile to="job-history">
            <v-list-tile-content>
              Jobs
            </v-list-tile-content>
          </v-list-tile>

          <v-list-tile to="target-history">
            <v-list-tile-content>
              Targets
            </v-list-tile-content>
          </v-list-tile>
        </v-list-group>
      </v-list>
    </v-navigation-drawer>

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
export default {
  name: 'App',
  components: {
  },
  data () {
    return {
      projects: [
        { name: 'Weather', description: 'dashboard' },
        { name: 'SFTP Upload', description: 'account_box' },
        { name: 'Example', description: 'gavel' }
      ]
    }
  }
}
</script>
