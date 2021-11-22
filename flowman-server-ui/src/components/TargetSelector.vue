<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="2">
        <v-subheader>Targets</v-subheader>
      </v-col>
      <v-col cols="9">
        <v-select
          v-model="value"
          :items="targets"
          chips
          label="Filter by Build Target"
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
