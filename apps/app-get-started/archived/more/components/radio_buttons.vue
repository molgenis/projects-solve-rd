<template>
  <div>
    <div
        class="radio-button-group"
        v-for="option in options"
        v-bind:key="option.id"
    >
        <div class="radio-button-item">
            <input
                type="radio"
                v-bind:id="'radio-' + option.id"
                v-bind:value="option.label"
                v-bind:name="name"
                v-bind:checked="option.default"
                @change="updateValue($event.target.value)"
            />
            <label v-bind:for="'radio-' + option.id">{{ option.label }}</label>
        </div>
    </div>
  </div>
</template>

<script>
export default {
  emits: ['change', 'value'],
  props: {
    options: Object,
    name: String
  },
  methods: {
    updateValue (value) {
      this.$emit('change', value)
    }
  }
}
</script>

<style lang="scss">
@import "../styles/variables";

.radio-button-group {
    display: inline-flex;
    flex-direction: row;

    .radio-button-item {
        box-sizing: border-box;
        margin: 6px;
        position: relative;

        input[type="radio"] {
            position: absolute;
            width: 1px;
            margin: 1px;
            margin: -1px;
            clip: rect(0, 0, 0, 0);
            clip: rect(0 0 0 0);
            overflow: hidden;
            white-space: nowrap;

            &:checked + label {
                cursor: pointer;
                border-color: $blue-900;
                background-color: $blue-900;
                color: $gray-050;
                border-bottom-width: 3px;
            }
            &:hover,
            &:focus {
                + label {
                    border-color: $blue-900;
                    color: $blue-900;
                }

                &:checked + label {
                    color: $gray-050;
                }
            }
        }

        label {
            padding: 12px;
            border: 1px solid $gray-300;
            background-color: $gray-000;
            cursor: pointer;
        }
    }
}
</style>
