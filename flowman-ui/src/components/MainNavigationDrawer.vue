<template>
  <v-navigation-drawer app>
    <v-list-item>
      <v-list-item-content>
        <v-list-item-title class="title">
          Flowman
        </v-list-item-title>
      </v-list-item-content>
    </v-list-item>

    <v-divider></v-divider>

    <v-list nav>
      <v-list-item :to="{name: 'system'}">
        <v-list-item-action>
          <v-icon>info</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          System
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'namespace'}">
        <v-list-item-action>
          <v-icon>home</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Namespace
        </v-list-item-title>
      </v-list-item>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>work</v-icon>
            </v-list-item-action>
            <v-list-item-title>Projects</v-list-item-title>
          </v-list-item>
        </template>

        <v-list-item
          v-for="project in projects"
          :key="project.basedir"
          :to="{ name: 'project', params:{ name: project.name}}"
        >
          <v-list-item-content>
            <v-list-item-title>{{ project.name }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>history</v-icon>
            </v-list-item-action>
            <v-list-item-title>
              History
            </v-list-item-title>
          </v-list-item>
        </template>

        <v-list-item :to="{name: 'job-history', params:{ projectName: ''}}">
          <v-list-item-content>
            Jobs
          </v-list-item-content>
        </v-list-item>

        <v-list-item :to="{name: 'target-history', params:{ projectName: ''}}">
          <v-list-item-content>
            Targets
          </v-list-item-content>
        </v-list-item>
      </v-list-group>
    </v-list>
  </v-navigation-drawer>
</template>

<script>
  export default {
    name: 'MainNavigationDrawe',

    data() {
      return {
        projects: []
      }
    },

    mounted() {
      this.$api.listProjects().then(response => this.projects = response)
    },

    methods: {
    }
  }
</script>
