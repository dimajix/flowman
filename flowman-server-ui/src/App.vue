<template>
  <v-app>
    <v-app-bar
      app
      fixed
      clipped-left
      class="brand-blue white--text"
    >
      <v-app-bar-nav-icon
        @click.stop="expand = !expand"
      >
        <v-icon class="white--text">menu</v-icon>
      </v-app-bar-nav-icon>

      <v-toolbar-title
      >
        <img src="@/assets/flowman-logo.png" alt="Flowman"/>
        History Server
      </v-toolbar-title>

      <v-layout
        fill-height
        align-start
      >
        <v-row>
          <v-col cols="1">
            <v-spacer/>
          </v-col>

          <v-col cols="4">
            <v-layout>
              <v-flex shrink>
                <v-list-item-content class="white--text">
                  Project&nbsp;&nbsp;
                </v-list-item-content>
              </v-flex>
              <v-flex>
                <v-select
                  v-model="project"
                  :items="projects"
                  solo
                  label="Project Name"
                  append-icon="expand_more"
                  clear-icon="clear"
                  background-color="indigo"
                >
                </v-select>
              </v-flex>
            </v-layout>
          </v-col>
        </v-row>
      </v-layout>
    </v-app-bar>

    <main-navigation-drawer
      :expand="expand"
      :project="project"
    >
    </main-navigation-drawer>

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
  },

  data() {
    return {
      expand: true,
      project: "",
      projects: [],
    };
  },

  watch: {
    project: function () { this.refreshRoute() },
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.$api.getJobCounts('project')
        .then(response => {
          this.projects =  Object.keys(response.data).sort((l,r) => l >= r).filter(l => l.trim() !== "")
          if (this.projects.length > 0)
            this.project = this.projects[0]
          else
            this.project = ""
        })
    },

    refreshRoute() {
      let name = this.$route.name
      let params = this.$route.params
      if (('project' in params) && (params.project !== this.project)) {
        params.project = this.project
        this.$router.push({name: name, params: params})
      }
    }
  }
};
</script>
