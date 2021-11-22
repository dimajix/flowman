<template>
  <v-container fluid>
    <v-select
      v-model="value"
      :items="phases"
      chips
      label="Filter by Build Phase"
      multiple
      clearable
      deletable-chips
      @input='$emit("input", value)'
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'PhaseSelector',

  data() {
    return {
      value: [],
      phases: ["VALIDATE", "CREATE", "BUILD", "VERIFY", "TRUNCATE", "DESTROY"],
    };
  },

  mounted() {
  },
};
</script>

<style>
</style>
