<template>
  <v-dialog
    v-model="show"
  >
    <template v-slot:activator="slotProps">
      <slot name="activator" v-bind="slotProps"></slot>
    </template>
    <v-card elevation="2" class="flex-fill">
      <v-card-title>
        <v-col>Available Projects</v-col>
        <v-col class="text-right"><v-icon @click="reloadProjects()">refresh</v-icon></v-col>
      </v-card-title>
      <v-data-table
        dense
        :headers="headers"
        :items="projects"
        :items-per-page="20"
        class="elevation-1"
      >
        <template v-slot:item.actions="{ item }">
          <v-btn @click.stop="openProject(item.name)">
          <v-icon>
            work
          </v-icon>
            Open
          </v-btn>
        </template>
      </v-data-table>
    </v-card>
  </v-dialog>
</template>

<script>
export default {
  name: 'ProjectSelector',
  components: {},

  data: () => ({
    show: false,
    project: null,
    projects: [],
    headers: [
      { text: 'Name', align: 'start', value: 'name' },
      { text: 'Location', value: 'basedir' },
      { text: 'Actions', value: 'actions', sortable: false },
    ],
  }),

  props: {
    kernel: null
  },

  mounted() {
    this.reloadProjects()
  },

  methods: {
    reloadProjects() {
      this.$api.listProjects(this.kernel)
        .then(response => {
          this.projects = response.projects
        })
    },
    openProject(project) {
      project
    }
  }
}
</script>
