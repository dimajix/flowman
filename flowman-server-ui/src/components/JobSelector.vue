<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="11">
        <v-select
          v-model="value"
          :items="jobs"
          chips
          outlined
          label="Filter by Job Name"
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
