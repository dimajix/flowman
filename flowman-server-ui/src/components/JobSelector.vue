<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="12">
        <v-select
          v-model="value"
          :items="jobs"
          chips
          solo
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

  props: {
    projects: {
      type: Array,
      default: () => []
    }
  },

  watch: {
    projects: function() { this.getData() }
  },

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
      this.$api.getJobCounts('job', this.projects)
        .then(response => {
          this.jobs =  Object.keys(response.data).sort()
          this.value.splice(0, this.value.length)
        })
    }
  }
};
</script>

<style>
</style>
