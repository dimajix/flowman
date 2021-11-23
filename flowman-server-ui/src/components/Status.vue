<template>
  <v-chip
    label
    outlined
    :small="small"
    :color="getColor(status)"
  >
    <v-icon>{{ getIcon(status) }}</v-icon>&nbsp; {{ status }}
  </v-chip>
</template>

<script>
export default {
  name: 'Status',

  props: {
    status: { type: String },
    small: {
      type: Boolean,
      default: () => {
        return false
      }
    }
  },

  methods: {
    getColor(status) {
      if (status === "SUCCESS") {
        return "#609040"
      }
      else if (status === "SKIPPED") {
        return "#406090"
      }
      else if (status === "FAILED") {
        return "#904060"
      }
      else {
        return "#809080"
      }
    },

    getIcon(status) {
      if (status === "SUCCESS") {
        return "done_all"
      }
      else if (status === "SKIPPED") {
        return "fast_forward"
      }
      else if (status === "FAILED") {
        return "error"
      }
      else {
        return "warning_amber"
      }
    }
  },

};
</script>
