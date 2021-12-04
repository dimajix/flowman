<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="12">
        <v-select
          v-model="value"
          :items="targets"
          chips
          solo
          label="Filter by Build Target"
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
  name: 'TargetSelector',

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
      value: [],
      targets: [],
    }
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('target', this.projects)
        .then(response => {
          this.targets =  Object.keys(response.data)
          this.value.splice(0, this.value.length)
        })
    }
  }
};
</script>

<style>
</style>
