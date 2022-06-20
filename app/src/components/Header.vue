<template>
  <header :id="id" class="header" :style="imageSrc ? `background-image: url(${imageSrc})` : ''">
    <div class="header-content">
      <h1 :id="id + '-title'" class="header-title">
        {{ title }}
      </h1>
      <h2 :id="id + '-subtitle'" class="header-subtitle">
        {{ subtitle }}
      </h2>
    </div>
    <div class="header-filter" v-if="imageSrc" aria-hidden="true"></div>
  </header>
</template>

<script>
export default {
  name: 'PageHeader',
  props: {
    id: {
      type: String,
      required: true
    },
    title: {
      type: String,
      required: true,
      default: 'Example Title'
    },
    subtitle: {
      type: String,
      required: true,
      default: 'Example Subtitle'
    },
    imageSrc: {
      type: String,
      required: false
    }
  }
}
</script>

<style lang="scss">
.header {
  position: relative;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #282c32;
  padding: 24px;
  height: 15em;
  max-height: 15em;
  
  .header-content {
    width: 90%;
    margin: 0 auto;
    z-index: 1;

    h1, h2 {
      margin: 0;
      line-height: 1.3;
    }

    .header-title {
      font-size: 13pt;
      text-transform: uppercase;
      letter-spacing: 4px;
      font-weight: bold;
      color: hsl(216, 11%, 90%);
    }

    .header-subtitle {
      font-size: 32pt;
      font-weight: 200;
      color: hsl(216, 11%, 98%);
    }
  }

  &.header-bg-image {
    background-size: cover;
    background-position: center;

    .header-filter {
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background-color: hsla(0, 0%, 0%, 0.45);
    }
  }
}
</style>
