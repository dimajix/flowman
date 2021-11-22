<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="2">
        <v-subheader>Jobs</v-subheader>
      </v-col>
      <v-col cols="9">
        <v-select
          v-model="value"
          :items="jobs"
          chips
          label="Filter by Job Name"
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
  name: 'JobSelector',

  data() {
    return {
      jobs: [],
      value: [],
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.$api.getJobCounts('job')
        .then(response => {
          this.jobs =  Object.keys(response.data)
        })
    }
  }
};
</script>

<style>
</style>
