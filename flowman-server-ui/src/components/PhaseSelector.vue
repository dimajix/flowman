<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="11">
        <v-select
          v-model="value"
          :items="phases"
          chips
          outlined
          label="Filter by Build Phase"
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
