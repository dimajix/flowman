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
      else if (status === "SUCCESS_WITH_ERRORS") {
        return "#B0B060"
      }
      else if (status === "RUNNING") {
        return "#80B060"
      }
      else if (status === "SKIPPED") {
        return "#406090"
      }
      else if (status === "FAILED" || status === "ABORTED") {
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
      else if (status === "SUCCESS_WITH_ERRORS") {
        return "download_done"
      }
      else if (status === "RUNNING") {
        return "directions_run"
      }
      else if (status === "SKIPPED") {
        return "fast_forward"
      }
      else if (status === "FAILED" || status === "ABORTED") {
        return "error"
      }
      else {
        return "warning_amber"
      }
    }
  },

};
</script>
