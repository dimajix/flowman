<template>
  <v-container fluid>
    <v-select
      v-model="selection"
      :items="projects"
      chips
      label="Filter by Project Name"
      multiple
      clearable
      deletable-chips
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'ProjectSelector',

  props : {
    selection: [],
  },

  data() {
    return {
      projects: [],
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.$api.getJobCounts('project')
        .then(response => {
          this.projects =  Object.keys(response.data)
        })
    }
  }
};
</script>

<style>
</style>
