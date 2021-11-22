<template>
  <v-container fluid>
    <v-select
      v-model="value"
      :items="projects"
      chips
      label="Filter by Project Name"
      multiple
      clearable
      deletable-chips
      @input='$emit("input", value)'
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'ProjectSelector',

  data() {
    return {
      value: [],
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
