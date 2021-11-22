<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="11">
        <v-select
          v-model="value"
          :items="projects"
          chips
          outlined
          align="start"
          label="Filter by Project Name"
          multiple
          clearable
          deletable-chips
          append-icon="expand_more"
          clear-icon="clear"
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
