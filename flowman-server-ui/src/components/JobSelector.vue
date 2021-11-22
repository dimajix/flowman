<template>
  <v-container fluid>
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
