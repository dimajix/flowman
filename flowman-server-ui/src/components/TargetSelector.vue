<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="11">
        <v-select
          v-model="value"
          :items="targets"
          chips
          outlined
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
      this.$api.getTargetCounts('target')
        .then(response => {
          this.targets =  Object.keys(response.data)
        })
    }
  }
};
</script>

<style>
</style>
