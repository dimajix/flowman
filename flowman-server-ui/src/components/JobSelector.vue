<template>
  <v-container fluid>
    <v-select
      v-model="selection"
      :items="jobs"
      chips
      label="Filter by Job Name"
      multiple
      clearable
      deletable-chips
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'JobSelector',

  props : {
    selection: [],
  },

  data() {
    return {
      jobs: [],
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
