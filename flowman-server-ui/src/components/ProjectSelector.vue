<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="2">
        <v-subheader>Projects</v-subheader>
      </v-col>
      <v-col cols="9">
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
      </v-col>
    </v-row>
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
